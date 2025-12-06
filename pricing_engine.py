import math
from datetime import datetime, timezone
from typing import Optional

from scipy.stats import norm


def get_time_to_expiry(expiry_str: str) -> float:
    """
    Calculate years to expiry from ISO string.
    Returns 0 if parsing fails or expiry is in the past.
    """
    if not expiry_str:
        return 0.0
    try:
        end = datetime.fromisoformat(expiry_str.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        diff = (end - now).total_seconds()
        if diff <= 0:
            return 0.0
        # Convert seconds to years
        return diff / (365 * 24 * 60 * 60)
    except Exception:
        return 0.0


def calculate_fair_probability(
    current_price: float,
    strike_price: float,
    time_to_expiry_years: float,
    volatility: float = 0.6,
) -> float:
    """
    Black-Scholes for Cash-or-Nothing call: P(S > K at expiry).
    Assumptions:
    - r = 0 for short horizons
    - current_price, strike_price > 0
    - volatility: annualized (e.g., 0.6 for 60%)
    """
    if current_price <= 0 or strike_price <= 0:
        return 0.5

    if time_to_expiry_years <= 0:
        return 1.0 if current_price > strike_price else 0.0

    try:
        numerator = math.log(current_price / strike_price) - (
            0.5 * volatility * volatility * time_to_expiry_years
        )
        denominator = volatility * math.sqrt(time_to_expiry_years)
        d2 = numerator / denominator
        return float(norm.cdf(d2))
    except Exception:
        return 0.5

