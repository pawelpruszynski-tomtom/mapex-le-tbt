#!/usr/bin/env bash

CITY_NAME="Riyadh"
COUNTRY_CODE="QAT"
CSV_FILE_NAME="Routes2check_Riyadh_testing"
SAMPLE_ID="0273e3cc-a095-4e4a-aa33-b760433ed8fe"

SAMPLING_DIR="/home/pruszyns/analytics_tbt/data/tbt/sampling"

echo "CREATING sampling_samples.parquet"
python3 "./__generate_sampling_samples_parquet_from_csv.py" "$CSV_FILE_NAME" "$CITY_NAME"

echo "CREATING sampling_metadata.parquet"
python3 "./__generate_sampling_metadata_parquet.py" "$COUNTRY_CODE" "$CITY_NAME" "$SAMPLE_ID"

echo "Inspection dir is prepared for a new run."