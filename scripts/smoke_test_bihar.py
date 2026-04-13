from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from mgnrega_assets.pipeline import run_smoke_test


if __name__ == "__main__":
    run_smoke_test(state_code="05", state_name="BIHAR")
