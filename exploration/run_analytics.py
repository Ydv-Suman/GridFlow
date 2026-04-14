from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from exploration.pipeline_utils import OUTPUT_DIR, run_analytics


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run workflow analytics for phases 2-6.")
    parser.add_argument(
        "--merged-file",
        type=Path,
        default=Path("data/merged/energy_monthly.csv"),
        help="Merged CSV produced by scripts/merge_data.py",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    artifacts = run_analytics(args.merged_file)

    print(f"Saved analytics outputs to {OUTPUT_DIR}")
    print(
        "Regime shares: "
        + ", ".join(
            f"{name}={values['share_pct']:.1f}%"
            for name, values in artifacts.regime_statistics.items()
        )
    )
    print(
        f"Simulation savings: ${artifacts.simulation_summary['total_savings']:.2f} "
        f"({artifacts.simulation_summary['savings_pct']:.2f}%)"
    )


if __name__ == "__main__":
    main()
