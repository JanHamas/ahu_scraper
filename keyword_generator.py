import itertools
import string
import csv
from config import setting
from pathlib import Path

CHARS = string.ascii_lowercase   # a-z

def generate_keywords() -> None:
    combos = [''.join(c) for c in itertools.product(CHARS, repeat=3)]
    print(f"[KEYWORDS] Phase 1: generated {len(combos)} three-letter keywords (a-z)")

    rows = [{"keywords": kw} for kw in combos]

    # ensure directory exists
    csv_file: Path = setting.KEYWORDS_CSV_FILE
    csv_file.parent.mkdir(parents=True, exist_ok=True)

    # save CSV
    with open(csv_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["keywords"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"[KEYWORDS] Saved all keywords to {csv_file}")

# run
generate_keywords()
