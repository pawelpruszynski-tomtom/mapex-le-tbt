#!/usr/bin/env bash

echo "CREATING empty_critical_sections_with_mcp_feedback.parquet"
python3 "./_generate_empty_critical_sections_with_mcp_feedback.py"

echo "CREATING empty_inspection_critical_sections.parquet"
python3 "./_generate_empty_inspection_critical_sections.py"

echo "Creating empty_metadata.parquet"
python3 "./_generate_empty_metadata.py"

echo "Creating empty_inspection_routes.parquet"
python3 "./_generate_empty_parquet.py"

echo "Inspection dir is prepared for a new run."