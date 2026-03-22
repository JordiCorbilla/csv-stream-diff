from __future__ import annotations

import shutil
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


def after_scenario(context, scenario) -> None:  # type: ignore[no-untyped-def]
    temp_dir = getattr(context, "temp_dir", None)
    if temp_dir:
        shutil.rmtree(temp_dir, ignore_errors=True)
