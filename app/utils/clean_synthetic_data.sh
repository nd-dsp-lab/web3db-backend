#!/bin/bash

DATASET_DIR="../dataset"

# Remove synthetic_data directory (all generated hospital_day files)
rm -rf "${DATASET_DIR}/synthetic_data"

echo "All synthetic data removed."
