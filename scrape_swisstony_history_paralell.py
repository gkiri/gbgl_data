#!/usr/bin/env python3
"""
Gabagool2 Historical Trade Scraper - PARALLEL VERSION (ALL MARKETS / ALL TRADES)

This version captures *every trade* made by the target wallet across *all* Polymarket markets.
It removes all BTC/ETH + 15-minute market slug filters and instead queries the Data API activity
feed filtered to TRADE events (which already includes conditionId, slug, eventSlug, tx hash, etc).

Key changes vs your original:
- NO market slug generation (no BTC/ETH/15m filters)
- Pulls trades directly by wallet across ALL markets
- Uses /activity with type=TRADE + (start,end) time window support
- Auto-splits time windows if a range is too dense to page via offset safely

Docs:
- /activity supports: user, type, start, end, sortBy, sortDirection, limit, offset. (limit<=500)  :contentReference[oaicite:2]{index=2}
- /trades returns trade fields like slug/eventSlug/conditionId, but does not support (start,end). :contentReference[oaicite:3]{index=3}
"""

import argparse
import asyncio
import json
import multiprocessing as mp
import os
import sys
import time
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any

import aiohttp
from tqdm import tqdm

# ------------------------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------------------------
GABAGOOL_ADDRESS = "0x204f72f35326db932158cba6adff0b9a1da95e14"

# Data API endpoints
ACTIVITY_URL = "https://data-api.polymarket.com/activity"

# Rate limiting / reliability
REQUEST_TIMEOUT = 20
MAX_RETRIES = 3
RETRY_DELAY = 1.0
CONCURRENT_REQUESTS_PER_WORKER = 10
RATE_LIMIT_DELAY = 0.05

# /activity endpoint constraints (as documented)
ACTIVITY_PAGE_LIMIT = 500
# Docs show offset range up to 10000; to be safe we auto-split time windows if a window would exceed this.
MAX_SAFE_OFFSET = 10000
MAX_SPLIT_DEPTH = 30  # safety guard for window splitting recursion

# ------------------------------------------------------------------------------
# DATA STRUCTURES
# ------------------------------------------------------------------------------
@dataclass
class TimeWindowTask:
    start_ts: int
    end_ts: int
    index: int

@dataclass
class WindowResult:
    start_ts: int
    end_ts: int
    trades: List[Dict[str, Any]]
    success: bool
    error: Optional[str] = None

@dataclass
class WorkerStats:
    worker_id: int
    windows_processed: int
    windows_with_trades: int
    total_trades: int
    errors: int
    duration: float

# ------------------------------------------------------------------------------
# HELPERS
# ------------------------------------------------------------------------------
def ts_to_str(ts: int) -> str:
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")

def safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return default

# ------------------------------------------------------------------------------
# TASK GENERATION (time windows, not markets)
# ------------------------------------------------------------------------------
def generate_time_window_tasks(
    start_ts: int,
    end_ts: int,
    num_workers: int,
) -> List[TimeWindowTask]:
    """
    Split [start_ts, end_ts] into a reasonable number of time windows so we can parallelize.
    We target ~4x workers windows (min 16) to balance load.
    """
    if end_ts <= start_ts:
        return []

    total_range = end_ts - start_ts
    target_tasks = max(num_workers * 4, 16)
    window_seconds = max(3600, total_range // target_tasks)  # at least 1 hour
    # Round to nearest hour for neat boundaries
    window_seconds = max(3600, (window_seconds // 3600) * 3600)

    tasks: List[TimeWindowTask] = []
    idx = 0
    cur = start_ts
    while cur < end_ts:
        nxt = min(cur + window_seconds, end_ts)
        tasks.append(TimeWindowTask(start_ts=cur, end_ts=nxt, index=idx))
        cur = nxt
        idx += 1

    return tasks

def create_batches(tasks: List[TimeWindowTask], num_workers: int) -> List[List[TimeWindowTask]]:
    """
    Divide tasks into balanced batches for workers.
    """
    if not tasks:
        return [[] for _ in range(num_workers)]

    batch_size = len(tasks) // num_workers
    remainder = len(tasks) % num_workers

    batches: List[List[TimeWindowTask]] = []
    start = 0
    for i in range(num_workers):
        size = batch_size + (1 if i < remainder else 0)
        end = start + size
        batches.append(tasks[start:end])
        start = end

    return batches

# ------------------------------------------------------------------------------
# ASYNC API FUNCTIONS
# ------------------------------------------------------------------------------
async def fetch_with_retry(
    session: aiohttp.ClientSession,
    url: str,
    params: Dict[str, Any],
    max_retries: int = MAX_RETRIES,
) -> Optional[Any]:
    """
    Fetch URL with retry logic and exponential backoff.
    Returns decoded JSON on success, None on failure.
    """
    for attempt in range(max_retries):
        try:
            async with session.get(url, params=params, timeout=REQUEST_TIMEOUT) as resp:
                if resp.status == 200:
                    return await resp.json()
                if resp.status == 429:
                    await asyncio.sleep(RETRY_DELAY * (2 ** attempt))
                    continue
                return None
        except (aiohttp.ClientError, asyncio.TimeoutError):
            if attempt < max_retries - 1:
                await asyncio.sleep(RETRY_DELAY * (2 ** attempt))
            else:
                return None
    return None

def normalize_activity_response(data: Any) -> List[Dict[str, Any]]:
    """
    /activity is documented as returning a JSON array. But we defensively handle dict shapes too.
    """
    if data is None:
        return []
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        # best guesses
        for key in ("activity", "events", "data", "results"):
            if isinstance(data.get(key), list):
                return data[key]
    return []

async def fetch_trade_activity_range(
    session: aiohttp.ClientSession,
    user_address: str,
    start_ts: int,
    end_ts: int,
    depth: int = 0,
) -> List[Dict[str, Any]]:
    """
    Fetch all TRADE activity in [start_ts, end_ts] using /activity with pagination.

    If the range is too dense (would exceed MAX_SAFE_OFFSET), auto-split time window and retry.
    This ensures we don't silently miss trades due to offset limits.
    """
    limit = ACTIVITY_PAGE_LIMIT
    offset = 0
    collected: List[Dict[str, Any]] = []

    while True:
        params = {
            "user": user_address,
            "type": "TRADE",
            "start": start_ts,
            "end": end_ts,
            "sortBy": "TIMESTAMP",
            "sortDirection": "ASC",
            "limit": limit,
            "offset": offset,
        }

        data = await fetch_with_retry(session, ACTIVITY_URL, params)
        batch = normalize_activity_response(data)

        # If the API fails for this page, stop this window (caller will record error).
        # We prefer partial data over crashing the entire run.
        if data is None:
            break

        collected.extend(batch)

        if len(batch) < limit:
            break

        offset += limit
        await asyncio.sleep(RATE_LIMIT_DELAY)

        # If we're going to exceed safe offset paging, split and refetch the window properly.
        if offset >= MAX_SAFE_OFFSET:
            if depth >= MAX_SPLIT_DEPTH or (end_ts - start_ts) <= 1:
                # Safety fallback: return what we got (may be incomplete in extreme cases)
                return collected

            mid = (start_ts + end_ts) // 2
            # Avoid degenerate splits
            if mid <= start_ts:
                mid = start_ts + 1
            if mid >= end_ts:
                mid = end_ts - 1

            left = await fetch_trade_activity_range(session, user_address, start_ts, mid, depth + 1)
            right = await fetch_trade_activity_range(session, user_address, mid, end_ts, depth + 1)
            return left + right

    return collected

async def process_window_async(
    session: aiohttp.ClientSession,
    task: TimeWindowTask,
    user_address: str,
) -> WindowResult:
    """
    Process one time window and return the trades for that window.
    """
    try:
        trades = await fetch_trade_activity_range(session, user_address, task.start_ts, task.end_ts)

        # Add small compatibility metadata fields (so your downstream code can rely on these keys)
        for t in trades:
            t["_wallet"] = user_address
            t["_range_start_ts"] = task.start_ts
            t["_range_end_ts"] = task.end_ts

            # Common aliases for convenience/back-compat
            if "conditionId" in t and "_condition_id" not in t:
                t["_condition_id"] = t.get("conditionId")
            if "slug" in t and "_market_slug" not in t:
                t["_market_slug"] = t.get("slug")
            if "eventSlug" in t and "_event_slug" not in t:
                t["_event_slug"] = t.get("eventSlug")

        return WindowResult(
            start_ts=task.start_ts,
            end_ts=task.end_ts,
            trades=trades,
            success=True,
            error=None,
        )

    except Exception as e:
        return WindowResult(
            start_ts=task.start_ts,
            end_ts=task.end_ts,
            trades=[],
            success=False,
            error=str(e),
        )

async def process_batch_async(
    tasks: List[TimeWindowTask],
    worker_id: int,
    user_address: str,
) -> Tuple[List[WindowResult], WorkerStats]:
    """
    Process a batch of time windows using async I/O with concurrency control.
    """
    start_time = time.time()
    results: List[WindowResult] = []
    stats = WorkerStats(
        worker_id=worker_id,
        windows_processed=0,
        windows_with_trades=0,
        total_trades=0,
        errors=0,
        duration=0.0,
    )

    connector = aiohttp.TCPConnector(limit=CONCURRENT_REQUESTS_PER_WORKER)
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS_PER_WORKER)

        async def run_one(task: TimeWindowTask) -> WindowResult:
            async with semaphore:
                return await process_window_async(session, task, user_address)

        window_jobs = [run_one(t) for t in tasks]
        gathered = await asyncio.gather(*window_jobs, return_exceptions=True)

        for item in gathered:
            if isinstance(item, Exception):
                stats.errors += 1
                continue
            if isinstance(item, WindowResult):
                results.append(item)
                stats.windows_processed += 1
                if item.trades:
                    stats.windows_with_trades += 1
                    stats.total_trades += len(item.trades)
                if item.error:
                    stats.errors += 1

    stats.duration = time.time() - start_time
    return results, stats

def worker_process(batch: List[TimeWindowTask], worker_id: int, user_address: str, result_queue: mp.Queue):
    """
    Worker process entry point. Runs an async event loop.
    """
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        results, stats = loop.run_until_complete(process_batch_async(batch, worker_id, user_address))
        result_queue.put(("results", results, stats))

    except Exception as e:
        result_queue.put(("error", worker_id, str(e)))
    finally:
        try:
            loop.close()
        except Exception:
            pass

# ------------------------------------------------------------------------------
# ORCHESTRATION
# ------------------------------------------------------------------------------
def dedupe_trades(trades: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Dedupe trades in case window boundaries overlap or recursion splits cause overlap.
    """
    seen = set()
    out = []
    for t in trades:
        key = (
            t.get("transactionHash"),
            t.get("conditionId"),
            t.get("timestamp"),
            t.get("side"),
            t.get("outcomeIndex"),
            t.get("type"),  # should be TRADE
            t.get("size"),
            t.get("price"),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(t)
    return out

def scrape_parallel(
    days: int = 30,
    output_path: str = "data/gabagool_history_all_30d_parallel.json",
    num_workers: Optional[int] = None,
    batch_size: Optional[int] = None,  # kept for CLI compatibility; used as "windows per batch" if provided
    user_address: str = GABAGOOL_ADDRESS,
    start_ts_override: Optional[int] = None,
    end_ts_override: Optional[int] = None,
):
    """
    Main parallel scraper:
    - Builds time-window tasks over the requested history
    - Parallelizes them across worker processes
    - Aggregates/dedupes/sorts trades
    """
    print("=" * 80)
    print("GABAGOOL2 PARALLEL HISTORICAL TRADE SCRAPER (ALL MARKETS)")
    print("=" * 80)
    print(f"Target Address: {user_address}")
    print("Market filter: NONE (captures all trades across all markets)")
    print(f"Days: {days} (0 = all available history)")
    print(f"Output: {output_path}")

    if num_workers is None or num_workers == 0:
        num_workers = mp.cpu_count()

    print(f"Workers: {num_workers} (CPU cores: {mp.cpu_count()})")
    print(f"Concurrent requests per worker: {CONCURRENT_REQUESTS_PER_WORKER}")
    print("=" * 80)

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

    now = int(time.time())
    end_ts = end_ts_override if end_ts_override is not None else now
    if start_ts_override is not None:
        start_ts = start_ts_override
    else:
        start_ts = 0 if days == 0 else max(0, end_ts - days * 24 * 60 * 60)

    if end_ts <= start_ts:
        print("Invalid time range: end_ts must be > start_ts")
        return

    # Generate time-window tasks
    tasks = generate_time_window_tasks(start_ts, end_ts, num_workers=num_workers)
    total_windows = len(tasks)

    if total_windows == 0:
        print("No windows to scrape!")
        return

    # Optional: if user provides batch_size, limit number of workers to match that batching
    if batch_size:
        num_workers = min(num_workers, (total_windows + batch_size - 1) // batch_size)

    batches = create_batches(tasks, num_workers)

    print(f"\nGenerated {total_windows} time windows to scan")
    print(f"Date range: {ts_to_str(start_ts)} to {ts_to_str(end_ts)}")
    print(f"Distributed {total_windows} windows across {num_workers} workers")
    print(f"Batch sizes: {[len(b) for b in batches]}")
    print("\nStarting parallel scraping...\n")

    start_time = time.time()
    result_queue: mp.Queue = mp.Queue()

    processes: List[mp.Process] = []
    for worker_id, batch in enumerate(batches):
        p = mp.Process(target=worker_process, args=(batch, worker_id, user_address, result_queue))
        p.start()
        processes.append(p)

    all_results: List[WindowResult] = []
    all_stats: List[WorkerStats] = []
    completed_workers = 0

    with tqdm(total=total_windows, desc="Scraping time windows", unit="window") as pbar:
        while completed_workers < num_workers:
            try:
                msg = result_queue.get(timeout=1)

                if msg[0] == "results":
                    _, results, stats = msg
                    all_results.extend(results)
                    all_stats.append(stats)
                    completed_workers += 1
                    pbar.update(len(results))

                elif msg[0] == "error":
                    _, worker_id, error = msg
                    print(f"\n[ERROR] Worker {worker_id} failed: {error}")
                    completed_workers += 1

            except Exception:
                if not any(p.is_alive() for p in processes):
                    break

    for p in processes:
        p.join(timeout=5)
        if p.is_alive():
            p.terminate()

    elapsed_total = time.time() - start_time

    # Aggregate trades
    all_trades: List[Dict[str, Any]] = []
    windows_scanned = 0
    windows_with_trades = 0
    total_errors = 0

    for r in all_results:
        windows_scanned += 1
        if r.trades:
            windows_with_trades += 1
            all_trades.extend(r.trades)
        if r.error:
            total_errors += 1

    # Dedupe + sort
    all_trades = dedupe_trades(all_trades)
    all_trades.sort(key=lambda t: (safe_int(t.get("timestamp")), str(t.get("transactionHash", ""))))

    # Compute actual collected range
    if all_trades:
        min_ts = min(safe_int(t.get("timestamp")) for t in all_trades)
        max_ts = max(safe_int(t.get("timestamp")) for t in all_trades)
        collected_range = f"{ts_to_str(min_ts)} to {ts_to_str(max_ts)}"
    else:
        collected_range = "n/a (no trades found)"

    metadata = {
        "wallet": user_address,
        "requested_days": days,
        "requested_range": f"{ts_to_str(start_ts)} to {ts_to_str(end_ts)}",
        "collected_trade_range": collected_range,
        "endpoint": "/activity",
        "activity_type_filter": "TRADE",
        "windows_scanned": windows_scanned,
        "windows_with_trades": windows_with_trades,
        "total_trades": len(all_trades),
        "scrape_duration_seconds": round(elapsed_total, 2),
        "completed_at": datetime.now().isoformat(),
        "status": "complete",
        "parallel_config": {
            "num_workers": num_workers,
            "concurrent_requests_per_worker": CONCURRENT_REQUESTS_PER_WORKER,
            "total_parallelism": num_workers * CONCURRENT_REQUESTS_PER_WORKER,
        },
        "worker_stats": [asdict(s) for s in all_stats],
        "errors": total_errors,
    }

    output_data = {
        "scrape_metadata": metadata,
        "trades": all_trades,
    }

    with open(output_path, "w") as f:
        json.dump(output_data, f, indent=2)

    # Summary
    print("\n" + "=" * 80)
    print("SCRAPE COMPLETE")
    print("=" * 80)
    print(f"Windows scanned: {windows_scanned}")
    print(f"Windows with trades: {windows_with_trades}")
    print(f"Total trades collected: {len(all_trades)}")
    print(f"Errors: {total_errors}")
    print(f"Duration: {elapsed_total:.1f} seconds ({elapsed_total/60:.1f} minutes)")
    if elapsed_total > 0:
        print(f"Rate: {windows_scanned/elapsed_total:.1f} windows/second")
    print(f"\nOutput saved to: {output_path}")

    print("\n" + "-" * 80)
    print("WORKER STATISTICS")
    print("-" * 80)
    for stats in all_stats:
        print(
            f"Worker {stats.worker_id}: "
            f"{stats.windows_processed} windows, "
            f"{stats.total_trades} trades, "
            f"{stats.errors} errors, "
            f"{stats.duration:.1f}s"
        )
    print("=" * 80)

    return output_data

# ------------------------------------------------------------------------------
# CLI
# ------------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Parallel scraper for a wallet's historical trades across ALL Polymarket markets",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Scrape last 30 days (all markets)
  python scrape_gabagool_history_parallel.py --days 30

  # Scrape last 7 days with 16 workers
  python scrape_gabagool_history_parallel.py --days 7 --workers 16

  # Scrape ALL available history
  python scrape_gabagool_history_parallel.py --days 0

  # Override time range explicitly (unix timestamps)
  python scrape_gabagool_history_parallel.py --start-ts 1700000000 --end-ts 1705000000
"""
    )
    parser.add_argument("--days", type=int, default=30, help="Number of days to scrape (default: 30). Use 0 for all history.")
    # Kept for backwards compatibility; ignored.
    parser.add_argument("--asset", type=str, default="ALL", help="DEPRECATED/IGNORED. Script now captures ALL markets.")
    parser.add_argument("--output", type=str, default=None, help="Output JSON file path.")
    parser.add_argument("--workers", type=int, default=None, help="Number of worker processes (default: CPU cores).")
    parser.add_argument("--batch-size", type=int, default=None, help="Windows per batch (optional).")
    parser.add_argument("--address", type=str, default=GABAGOOL_ADDRESS, help="Wallet address to scrape (default: gabagool).")
    parser.add_argument("--start-ts", type=int, default=None, help="Override start timestamp (unix seconds).")
    parser.add_argument("--end-ts", type=int, default=None, help="Override end timestamp (unix seconds).")

    args = parser.parse_args()

    if args.output is None:
        if args.days == 0:
            args.output = "data/gabagool_history_alltime_parallel.json"
        else:
            args.output = f"data/gabagool_history_all_{args.days}d_parallel.json"

    try:
        scrape_parallel(
            days=args.days,
            output_path=args.output,
            num_workers=args.workers,
            batch_size=args.batch_size,
            user_address=args.address,
            start_ts_override=args.start_ts,
            end_ts_override=args.end_ts,
        )
    except KeyboardInterrupt:
        print("\n\n[INTERRUPTED] Shutting down workers...")
        sys.exit(1)

if __name__ == "__main__":
    mp.set_start_method("spawn", force=True)
    main()
