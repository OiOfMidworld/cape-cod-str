"""
transform/town_normalizer.py

Normalizes town names from any source to the 15 canonical Barnstable County names.
Uses a CSV lookup first, then falls back to rapidfuzz for fuzzy matching.

Usage:
    from transform.town_normalizer import normalize_town

    normalize_town("Hyannis")        -> "Barnstable"
    normalize_town("P-Town")         -> "Provincetown"
    normalize_town("harwich port")   -> "Harwich"
    normalize_town("Plymouthe")      -> None  (not a Cape town)
"""

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

LOOKUP_PATH = Path(__file__).parent.parent / "data" / "reference" / "town_name_lookup.csv"

CANONICAL_TOWNS = {
    "Barnstable", "Bourne", "Brewster", "Chatham", "Dennis", "Eastham",
    "Falmouth", "Harwich", "Mashpee", "Orleans", "Provincetown",
    "Sandwich", "Truro", "Wellfleet", "Yarmouth",
}

# Module-level cache so we only read the CSV once
_lookup: dict = {}


def _load_lookup() -> dict:
    """Load the town name lookup CSV into a dict {raw_name_lower: canonical_name}."""
    global _lookup
    if _lookup:
        return _lookup

    if not LOOKUP_PATH.exists():
        logger.warning(f"Town lookup CSV not found at {LOOKUP_PATH}")
        return {}

    df = pd.read_csv(LOOKUP_PATH)
    _lookup = {
        row["raw_name"].strip().lower(): row["canonical_name"]
        for _, row in df.iterrows()
    }
    logger.debug(f"Loaded {len(_lookup)} town name mappings")
    return _lookup


def normalize_town(raw_name: str, fuzzy_threshold: int = 85):
    raw_name = ' '.join(raw_name.split())  # collapses multiple spaces
    """
    Normalize a raw town name string to a canonical Barnstable County town.

    Resolution order:
        1. Exact match (case-insensitive) against lookup table
        2. Already a canonical name
        3. Fuzzy match against canonical names (requires rapidfuzz)
        4. Returns None if no confident match found

    Args:
        raw_name:        Raw town name string from any source
        fuzzy_threshold: Minimum rapidfuzz score to accept (0-100, default 85)

    Returns:
        Canonical town name string, or None if no match
    """
    if not raw_name or not isinstance(raw_name, str):
        return None

    cleaned = raw_name.strip()
    lookup = _load_lookup()

    # 1. Exact match in lookup table
    match = lookup.get(cleaned.lower())
    if match:
        return match

    # 2. Already a canonical name
    if cleaned.title() in CANONICAL_TOWNS:
        return cleaned.title()

    # 3. Fuzzy match fallback
    try:
        from rapidfuzz import process, fuzz
        result = process.extractOne(
            cleaned,
            CANONICAL_TOWNS,
            scorer=fuzz.WRatio,
            score_cutoff=fuzzy_threshold,
        )
        if result:
            matched_name, score, _ = result
            logger.debug(f"Fuzzy matched '{cleaned}' -> '{matched_name}' (score={score})")
            return matched_name
    except ImportError:
        logger.warning("rapidfuzz not installed — fuzzy matching unavailable")

    logger.warning(f"Could not normalize town name: '{raw_name}'")
    return None


def normalize_series(series: pd.Series, fuzzy_threshold: int = 85) -> pd.Series:
    """
    Apply normalize_town to an entire pandas Series.
    Useful for batch normalization in ingestion scripts.

    Args:
        series:          pandas Series of raw town name strings
        fuzzy_threshold: Minimum rapidfuzz score to accept

    Returns:
        Series of canonical town names (None where no match found)
    """
    return series.apply(lambda x: normalize_town(x, fuzzy_threshold))
