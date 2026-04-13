# pyright: reportMissingImports=false

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from mgnrega_assets.pipeline import run_pipeline


if __name__ == "__main__":
    # Bihar state code: 05
    run_pipeline({"05": "BIHAR"}, max_workers=12, resume=False, reset_cp=False)
