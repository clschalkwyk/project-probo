.PHONY: blocknumber blocknumber-json update-stablecoins update-exchanges extract-wallet-data extract-wallet-data-90d extract-wallet-data-all-time extract-120 extract-with-fanout analyze-extractions run-api

blocknumber:
	python3 -m probo.cli blocknumber

blocknumber-json:
	python3 -m probo.cli blocknumber --json

update-stablecoins:
	python3 scripts/update_stablecoins.py

update-exchanges:
	python3 scripts/update_exchanges.py

extract-wallet-data:
	python3 scripts/extract_wallet_data.py

extract-wallet-data-90d:
	python3 scripts/extract_wallet_data.py --days 90

extract-wallet-data-all-time:
	python3 scripts/extract_wallet_data.py --include-all-time-count

extract-120:
	python3 scripts/extract_wallet_data.py --days 120 --max-total-transfers 1000

extract-with-fanout:
	python3 scripts/extract_wallet_data.py --days 120 --fanout-levels 5 --fanout-base-days 30 --fanout-base-tx 100 --fanout-decay 0.5

analyze-extractions:
	python3 scripts/analyze_extractions.py

run-api:
	uvicorn api.main:app --reload
