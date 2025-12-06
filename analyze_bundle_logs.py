#!/usr/bin/env python3
"""
Analyze bundle arbitrage JSONL logs.
Usage:
    python3 analyze_bundle_logs.py data/bundle_logs/bundle_session_<ts>.jsonl
If no path provided, will try to use the latest bundle_session_*.jsonl in data/bundle_logs.
"""

import argparse
import json
from pathlib import Path
from statistics import mean


def load_events(path: Path):
    events = []
    with path.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                events.append(json.loads(line))
            except Exception:
                continue
    return events


def summarize(events):
    sweeps = [e for e in events if e.get("event_type") == "sweep_result"]
    attempts = [e for e in events if e.get("event_type") == "sweep_attempt"]
    fills = [e for e in events if e.get("event_type") == "fill"]

    total_sweeps = len(sweeps)
    both_filled = sum(1 for e in sweeps if e.get("yes_fills", 0) > 0 and e.get("no_fills", 0) > 0)
    yes_only = sum(1 for e in sweeps if e.get("yes_fills", 0) > 0 and e.get("no_fills", 0) == 0)
    no_only = sum(1 for e in sweeps if e.get("no_fills", 0) > 0 and e.get("yes_fills", 0) == 0)

    avg_bundle_attempt = mean([e.get("bundle_cost", 0) for e in attempts]) if attempts else 0
    avg_bundle_filled = mean([e.get("bundle_cost", 0) for e in sweeps if (e.get("yes_fills", 0) + e.get("no_fills", 0)) > 0]) if sweeps else 0

    total_yes_vol = sum(e.get("yes_volume", 0) for e in sweeps)
    total_no_vol = sum(e.get("no_volume", 0) for e in sweeps)

    imbalance_samples = [e.get("position", {}).get("imbalance", 0) for e in sweeps]
    avg_imbalance = mean(imbalance_samples) if imbalance_samples else 0

    print("Bundle Log Summary")
    print("-------------------")
    print(f"Total sweep attempts: {len(attempts)}")
    print(f"Total sweep results : {total_sweeps}")
    print(f"  Both legs filled  : {both_filled}")
    print(f"  YES-only fills    : {yes_only}")
    print(f"  NO-only fills     : {no_only}")
    print(f"Average bundle (attempts): {avg_bundle_attempt:.4f}")
    print(f"Average bundle (filled)  : {avg_bundle_filled:.4f}")
    print(f"Total volume YES / NO    : {total_yes_vol:.1f} / {total_no_vol:.1f}")
    print(f"Average imbalance        : {avg_imbalance*100:.2f}%")
    print(f"Total fills logged       : {len(fills)}")


def main():
    parser = argparse.ArgumentParser(description="Analyze bundle JSONL logs")
    parser.add_argument("logfile", nargs="?", help="Path to bundle_session_*.jsonl")
    args = parser.parse_args()

    if args.logfile:
        path = Path(args.logfile)
    else:
        # pick latest bundle_session_*.jsonl
        logs = sorted(Path("data/bundle_logs").glob("bundle_session_*.jsonl"), reverse=True)
        if not logs:
            print("No bundle logs found.")
            return
        path = logs[0]

    if not path.exists():
        print(f"Log file not found: {path}")
        return

    events = load_events(path)
    if not events:
        print("No events parsed.")
        return

    summarize(events)


if __name__ == "__main__":
    main()

