from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from exploration.pipeline_utils import (
    MODEL_DIR,
    OUTPUT_DIR,
    run_analytics,
    train_price_forecaster,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train and export phase-7 models.")
    parser.add_argument(
        "--merged-file",
        type=Path,
        default=Path("data/merged/energy_monthly.csv"),
        help="Merged CSV used for training.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    analytics = run_analytics(args.merged_file)
    metrics = train_price_forecaster(analytics.labeled_data)

    print(f"Saved models to {MODEL_DIR}")
    print(f"Saved model metrics to {OUTPUT_DIR / 'forecast_metrics.json'}")
    print(
        "Forecaster metrics: "
        + ", ".join(f"{metric}={value:.3f}" for metric, value in metrics.items())
    )


if __name__ == "__main__":
    main()
