from mgnrega_assets.pipeline import run_pipeline


if __name__ == "__main__":
    # Bihar state code: 05
    run_pipeline({"05": "BIHAR"}, max_workers=40)
