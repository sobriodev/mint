#!/bin/bash
# Performs basic Database CLI regression tests.
# When running locally, the script must be invoked from the main repository's directory.

# Source common variables and functions
if [[ "true" == "$TRAVIS" ]]
then
  source "$TRAVIS_BUILD_DIR"/script/common.sh
else
  source ./script/common.sh
fi

# Database parameters
readonly CLI=target/release/cli
readonly TEMP_DIR=/tmp
readonly DB_NAME=CLI_Regression_DB
readonly DB_PATH=$TEMP_DIR/$DB_NAME

# Clean workspace
rm -rf $DB_PATH

# Create an empty database
output=$($CLI create --name $DB_NAME --directory $TEMP_DIR --json)
status=$(echo "$output" | jq ".status == 0 and .data.path == \"$DB_PATH\"")
assert_jq "$status" "Test database has been created" "Unable to create test database" "$output"

info "PASSED"