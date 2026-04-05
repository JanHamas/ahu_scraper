import itertools
import string
from scraper.logger import get_logger

log = get_logger()

CHARS = string.ascii_lowercase   # a-z


class KeywordGenerator:
    """
    Adaptive keyword generator for AHU company search.

    Strategy:
    - AHU requires minimum 3 characters per keyword.
    - Start with all 3-letter combos: aaa -> zzz (17,578 total)
    - If a keyword return > OVERFLOW_LIMIT results -> expand to 4-letter
    - Discard keywords with 0 or 1 result (singleton rule: task required > 2)
    - Keep keywords with 2-OVERFLOW_LIMIT results and scrape all pages

    why this works:
    - reCAPTCHA v3 is valid 2 minutes (~8 pages at 10s/page = 160 results)
    - Keeping each keyword < 160 result means one token cover the whole keyword
    - Expanding deep only when needed = minium total keywords
    """

    @staticmethod
    def generate_3letter() -> list[str]:
        """All 17,576 combinations: aaa - zzz"""
        combos = [''.join(c) for c in itertools.product(CHARS, repeat=3)]
        log.info(f"[KEYWORDS] Generated {len(combos)} three-letter keywords")
        return combos
    
    @staticmethod
    def expand(prefix: str) -> list[str]:
        """
        Expand a keyword by appending one more letter.
        'ind' -> ['inda', 'indb', ..., 'indz']
        'inda' -> ['indaa, 'indab', ..., 'indaz']
        """
        expanded = [prefix + c for c in CHARS]
        log.debug(f"[KEYWORDS] Expanded '{prefix}' -> {len(expanded)} keywords")
        return expanded