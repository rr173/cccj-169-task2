"""Device-clock drift estimation and correction.

The correction is an exact rational affine map:

    corrected_ts = round(device_ts * a + b)        a, b are Fractions

Estimated by least squares over (device_ts, server_ts) pairs using exact
integer/rational arithmetic, so the same input always yields the same
correction (reproducible timelines).
"""
from fractions import Fraction

MAX_PLAUSIBLE_SKEW = Fraction(1, 2)  # reject fits with |a-1| > 50%


def estimate_correction(pairs):
    """pairs: iterable of (device_ts, server_ts). Returns (a, b) as Fractions."""
    pts = sorted({(int(d), int(s)) for d, s in pairs})
    if not pts:
        return Fraction(1), Fraction(0)
    if len(pts) < 2:
        return Fraction(1), Fraction(pts[0][1] - pts[0][0])

    n = len(pts)
    xs = [Fraction(d) for d, _ in pts]
    ys = [Fraction(s) for _, s in pts]
    mx = sum(xs) / n
    my = sum(ys) / n
    sxx = sum((x - mx) ** 2 for x in xs)
    if sxx == 0:  # device clock never advanced -> cannot fit skew
        return Fraction(1), _median_offset(xs, ys)
    sxy = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    a = sxy / sxx
    b = my - a * mx
    if a <= 0 or abs(a - 1) > MAX_PLAUSIBLE_SKEW:
        # Physically implausible fit; fall back to identity skew + median offset.
        return Fraction(1), _median_offset(xs, ys)
    return a, b


def _median_offset(xs, ys):
    offs = sorted(y - x for x, y in zip(xs, ys))
    return offs[len(offs) // 2]


def correct(a, b, device_ts):
    """Apply correction; deterministic rounding (half-to-even on exact rationals)."""
    return int(round(a * int(device_ts) + b))


def frac_to_str(f):
    return "%d/%d" % (f.numerator, f.denominator)
