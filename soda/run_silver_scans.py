"""CLI entry point to run programmatic Soda Core quality scans for Silver layer."""

import sys
from pathlib import Path

# Ensure project root is in python path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from src.quality.soda_validator import run_silver_scan
from src.utils.logger import logger


def main() -> None:
    """Run Soda Core quality scans for all Silver tables."""
    contracts = [
        ("silver_metadata", "soda/contracts/silver_metadata_contract.yml"),
        ("silver_metrics", "soda/contracts/silver_metrics_contract.yml"),
        ("silver_prices", "soda/contracts/silver_prices_contract.yml"),
    ]

    has_failure = False
    for table_name, contract_path in contracts:
        try:
            run_silver_scan(table_name=table_name, contract_path=contract_path)
        except Exception as e:
            logger.error(f"Scan for {table_name} failed: {e}")
            has_failure = True

    if has_failure:
        sys.exit(1)


if __name__ == "__main__":
    main()
