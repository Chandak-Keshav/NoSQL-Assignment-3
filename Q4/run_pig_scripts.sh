#!/bin/bash
# run_pig_scripts.sh
# This script runs two Pig scripts in local mode, measures their execution time,
# and stores the outputs in the current directory.

# Ensure the Pig executable is in your PATH; adjust "pig" if needed.

# Function to run a Pig script and report the time taken.
run_pig_script () {
  local script_name="$1"
  local output_file="$2"

  echo "Running $script_name ..."
  local start_time=$(date +%s)
  
  # Run Pig in local mode, capturing both STDOUT and STDERR
  pig -x local "$script_name" > "$output_file" 2>&1
  
  local end_time=$(date +%s)
  local duration=$((end_time - start_time))
  
  echo "Finished running $script_name in $duration seconds."
  echo "Output stored in $output_file"
  echo ""
}

# Run the first Pig script (create_table.pig)
run_pig_script "create_table.pig" "create_table_output.txt"

# Run the second Pig script (Query1_CGPA.pig)
run_pig_script "Query1_CGPA.pig" "Query1_CGPA_output.txt"

# Optionally, report total runtime
echo "All scripts executed."
