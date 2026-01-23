#!/usr/bin/env python3
import csv
from pathlib import Path


def uppercase_addresses(csv_path: Path) -> None:
    temp_path = csv_path.with_suffix(".csv.tmp")

    with csv_path.open("r", newline="", encoding="utf-8") as infile, temp_path.open(
        "w", newline="", encoding="utf-8"
    ) as outfile:
        reader = csv.DictReader(infile)
        if not reader.fieldnames:
            raise ValueError("CSV file has no header row.")
        if "address" not in reader.fieldnames:
            raise ValueError('CSV file is missing required "address" column.')

        writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
        writer.writeheader()
        for row in reader:
            address = row.get("address")
            if address:
                row["address"] = address.upper()
            writer.writerow(row)

    temp_path.replace(csv_path)


def main() -> None:
    csv_path = Path("data/exchanges.csv")
    uppercase_addresses(csv_path)


if __name__ == "__main__":
    main()
