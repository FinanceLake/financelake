#!/bin/bash
# Clean the data_output folder and log files by removing all files while preserving directory structure

DATA_OUTPUT_DIR="./data_output"
LOG_DIR="./log"

echo "Starting cleanup of data_output and log directories..."
echo ""

# Function to clean a directory
clean_directory() {
    local dir=$1
    local dir_name=$2
    
    # Check if directory exists
    if [ ! -d "$dir" ]; then
        echo "ℹ $dir_name directory does not exist, skipping..."
        return 0
    fi
    
    # Count files before cleaning
    local total_files=$(find "$dir" -type f | wc -l)
    
    if [ $total_files -eq 0 ]; then
        echo "ℹ No files to clean in $dir_name"
        return 0
    fi
    
    # Remove all files while preserving directories
    find "$dir" -type f -delete
    
    # Verify cleanup
    local remaining_files=$(find "$dir" -type f | wc -l)
    
    if [ $remaining_files -eq 0 ]; then
        echo "✓ Successfully removed $total_files files from $dir_name"
        return 0
    else
        echo "✗ Error: $remaining_files files remain in $dir_name after cleanup"
        return 1
    fi
}

# Clean data_output
clean_directory "$DATA_OUTPUT_DIR" "data_output"
DATA_OUTPUT_STATUS=$?

# Clean logs
clean_directory "$LOG_DIR" "log"
LOG_STATUS=$?

echo ""

# Summary
if [ $DATA_OUTPUT_STATUS -eq 0 ] && [ $LOG_STATUS -eq 0 ]; then
    echo "✓ Cleanup completed successfully!"
    echo "✓ Directory structures preserved:"
    echo "  - data_output: $(find $DATA_OUTPUT_DIR -type d 2>/dev/null | wc -l) directories"
    echo "  - log: $(find $LOG_DIR -type d 2>/dev/null | wc -l) directories"
    exit 0
else
    echo "✗ Cleanup completed with errors"
    exit 1
fi
