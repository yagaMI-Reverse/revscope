"""FX feed and integer currency conversion for revscope.

One module shared by everything that touches more than one currency: gen.py
mints presentment amounts with it, the product marts convert back with it,
the tests pin it to known examples.

Rates are integers scaled by RATE_SCALE (1e8), never floats. The reason is
not purity. The same conversion runs twice -- once here in Python (generator
ground truth) and once in SQL (the marts) -- and two float pipelines drift
apart in the last cent. A reconciliation allowed to be off by a cent stops
catching real bugs, so both sides do integer arithmetic and agree bit for
bit. The float math below only shapes the rate path; every rate is frozen to
an integer before any money touches it.

A rate is "minor units of local currency per 1 USD" on a given UTC day.
The reporting currency is USD, because that is what the existing benches
already state their totals in.
"""

import math
import random
from datetime import date, timedelta

RATE_SCALE = 100_000_000
FX_SEED = 4242
FX_START = date(2021, 8, 1)      # same first day as the event stream
REPORTING_CURRENCY = "usd"

# Country -> presentment currency. The country list is the one gen.py has
# always used; only the currency mapping is new, so the customer mix (and
# every number published from it) is untouched.
CURRENCY_BY_COUNTRY = {
    "US": "usd", "DE": "eur", "NL": "eur", "FR": "eur", "ES": "eur",
    "GB": "gbp", "CA": "cad", "AU": "aud", "PL": "pln", "KZ": "kzt",
}

# currency -> (rate on 2021-08-01, rate on 2026-08-01, daily vol).
# Endpoints are set near the real moves of those pairs over the period so the
# historical-vs-current gap is a plausible size and not a straw man; the path
# between them is synthetic (see "Honest limits" in the README).
CURRENCY_PATH = {
    "usd": (1.0, 1.0, 0.0),
    "eur": (0.8430, 0.8600, 0.0035),
    "gbp": (0.7190, 0.7450, 0.0038),
    "cad": (1.2530, 1.3700, 0.0030),
    "aud": (1.3600, 1.5200, 0.0042),
    "pln": (3.8400, 3.6300, 0.0050),
    "kzt": (425.00, 540.00, 0.0045),
}
MEAN_REVERSION = 0.995   # deviation half-life ~138 days: wiggles, not a drunk


def n_days(start=FX_START, end=date(2026, 8, 1)):
    """Days in the feed, both ends inclusive."""
    return (end - start).days + 1


def build_rates(days, seed=FX_SEED, start=FX_START):
    """Deterministic daily rate table: {currency: [rate_scaled per day]}.

    Its own Random(seed) on purpose: the generator's main rng stream must not
    shift by a single draw, otherwise the whole published dataset (and every
    number measured on it) changes.
    """
    rng = random.Random(seed)
    out = {}
    for cur in sorted(CURRENCY_PATH):          # sorted: draw order must not
        lo, hi, vol = CURRENCY_PATH[cur]       # depend on dict literal order
        if vol == 0.0:
            out[cur] = [RATE_SCALE] * days
            continue
        log_lo, log_hi = math.log(lo), math.log(hi)
        dev, devs = 0.0, [0.0]
        for _ in range(days - 1):
            dev = dev * MEAN_REVERSION + vol * rng.gauss(0.0, 1.0)
            devs.append(dev)
        # Brownian bridge: tilt the accumulated deviation back to zero at the
        # last day. Without it the walk lands wherever it lands and the two
        # anchor rates above become decoration; with it they are the actual
        # first and last print, so the drift the benches report is the drift
        # this table was configured to have.
        end = devs[-1]
        devs = [d - end * i / (days - 1) for i, d in enumerate(devs)]
        series = []
        for i in range(days):
            trend = log_lo + (log_hi - log_lo) * i / (days - 1)
            series.append(int(round(math.exp(trend + devs[i]) * RATE_SCALE)))
        # FX has no weekend prints: Sat/Sun settle at Friday's close. Booking
        # a Sunday charge at an invented "Sunday rate" is the cheapest way to
        # disagree with the bank statement, so the feed carries Friday over.
        for i in range(days):
            wd = (start + timedelta(days=i)).weekday()
            if wd >= 5 and i - (wd - 4) >= 0:
                series[i] = series[i - (wd - 4)]
        out[cur] = series
    return out


def day_index(day, start=FX_START):
    return (day - start).days


def to_local(usd_cents, rate_scaled):
    """USD cents -> minor units of the local currency, half-up.

    SQL equivalent (bigint arithmetic, identical result):
        (amount_cents * rate_scaled + 50000000) / 100000000
    """
    return (usd_cents * rate_scaled + RATE_SCALE // 2) // RATE_SCALE


def to_usd(local_minor, rate_scaled):
    """Local minor units -> USD cents, half-up.

    SQL equivalent (bigint arithmetic, identical result):
        (amount_local * 100000000 + rate_scaled / 2) / rate_scaled

    Both sides truncate toward zero and every amount here is positive, so
    "+ half, then floor" is the same rounding rule in Postgres and Python.
    """
    return (local_minor * RATE_SCALE + rate_scaled // 2) // rate_scaled


def fmt_rate(rate_scaled):
    return f"{rate_scaled / RATE_SCALE:.4f}"
