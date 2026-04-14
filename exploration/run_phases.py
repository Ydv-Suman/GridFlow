from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from exploration.pipeline_utils import MODEL_DIR, OUTPUT_DIR, train_price_forecaster
from exploration.pipeline_utils import run_analytics
from scripts.merge_data import merge_all


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run GridFlow phases 1-7 without notebooks.")
    parser.add_argument(
        "--start",
        default=None,
        help="First year-month to include (YYYY-MM).",
    )
    parser.add_argument(
        "--end",
        default=None,
        help="Last year-month to include (YYYY-MM).",
    )
    parser.add_argument(
        "--merged-output",
        type=Path,
        default=Path("data/merged/energy_monthly.csv"),
        help="Merged phase-1 dataset destination.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    merged = merge_all(args.start, args.end)
    args.merged_output.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(args.merged_output, index=False)

    analytics = run_analytics(args.merged_output)
    metrics = train_price_forecaster(analytics.labeled_data)

    print(f"Saved merged dataset to {args.merged_output}")
    print(f"Saved analytics outputs to {OUTPUT_DIR}")
    print(f"Saved model artifacts to {MODEL_DIR}")
    print(
        f"Simulation savings: ${analytics.simulation_summary['total_savings']:.2f} "
        f"({analytics.simulation_summary['savings_pct']:.2f}%)"
    )
    print(
        "Price forecaster metrics: "
        + ", ".join(f"{metric}={value:.3f}" for metric, value in metrics.items())
    )


if __name__ == "__main__":
    main()
