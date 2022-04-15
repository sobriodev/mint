#!/bin/bash

# Color escape codes
readonly RED="\e[31m"
readonly GREEN="\e[32m"
readonly END_COLOR="\e[0m"

# Prints red error string with jq parsed JSON
# Usage: error_jq "Error string" $command_string_output
function error_jq() {
  echo -e "${RED}ERR/$1${END_COLOR}"
  echo "Got JSON object:"
  echo "$2" | jq '.'
}

# Prints green info string
# Usage: info "Success"
function info() {
  echo -e "${GREEN}INF/$1${END_COLOR}"
}

# Assert a particular condition with extra debug info
# Usage: assert_jq $status "Success" "Error" $command_string_output
function assert_jq() {
  if [[ "true" == "$1" ]]
  then
    info "$2"
  else
    error_jq "$3" "$4"
    exit 1
  fi
}