#!/usr/bin/env bash
#
# File:      kafka-move-leadership.sh
#
# Description
# ===========
#
# Generates a Kafka partition reassignment JSON snippet to STDOUT to scale up/down the replicas
# of the given topic. Run this script with `-h` to show detailed usage instructions.
#
#
# Requirements
# ============
# - Kafka 0.8.1.1 and later.
#
#
# Usage
# =====
#
# To show usage instructions run this script with `-h` or `--help`.
#
#
# Full workflow
# =============
#
# High-level overview
# -------------------
#
# 1. Use this script to generate a partition reassignment JSON file.
# 2. Start the actual reassignment operation via Kafka's `kafka-reassign-partitions.sh` script and this JSON file.
# 3. Monitor the progress of the reassignment operation with Kafka's `kafka-reassign-partitions.sh` script.
#
# Example
# -------
#
# NOTE: If you have installed the Confluent package of Kafka, then the CLI tool
#       `kafka-reassign-partitions.sh` is called `kafka-reassign-partitions`.
#
# Step 1 (generate reassignment JSON; this script):
#
#    $ kafka-replicate.sh --topic-name test-topic --replication 3  --first-broker-id 0 --last-broker-id 8 --zookeeper zookeeper1:2181 > partitions.json
#
# Step 2 (start reassignment process; Kafka built-in script):
#
#    $ kafka-reassign-partitions.sh --zookeeper zookeeper1:2181 --reassignment-json-file partitions.json --execute
#
# Step 3 (monitor progress of reassignment process; Kafka built-in script):
#
#    $ kafka-reassign-partitions.sh --zookeeper zookeeper1:2181 --reassignment-json-file partitions.json --verify


declare -r MYSELF=`basename $0`

print_usage() {
  echo "$MYSELF - generates a Kafka partition reassignment JSON snippet to move partition leadership away from a broker (details below)"
  echo
  echo "Usage: $MYSELF [OPTION]..."
  echo
  echo "    -t, --topic            the topic that you want to scale up/down its replicas"
  echo "    -r, --replication      the desired number of replicas. if set to zero it delete"
  echo "                           the topic. would do nothing if less than zero or greater"
  echo "                           than number of provided brokers"
  echo "                           it is also garanteed that in case of scale down, leader"
  echo "                           will not be removed"
  echo "                           kafka-topics CLI tool (ships with Kafka) not found in PATH'"
  echo "    -p, --kafka-bin-path   path to kafka binary directory, if not set, script will"
  echo "                           use its predefines, use it in case you got 'the "
  echo "                           kafka-topics CLI tool (ships with Kafka) not found in PATH'"
  echo "    -f, --first-broker-id  First (= lowest) Kafka broker ID in the cluster.  Used as"
  echo "                           the start index for the range of broker IDs from which"
  echo "                           replacement brokers will be randomly selected.  Example: 0"
  echo "    -l, --last-broker-id   Last (= highest) Kafka broker ID in the cluster.  Used as"
  echo "                           the end index for the range of broker IDs from which"
  echo "                           replacement brokers will be randomly selected.  Example: 8" 
  echo "    -x, --exclude          exclude given id from target lists.ignored if used with "
  echo "                           --replace-with"
  echo "                           <--first-brocker-id|--last-broker-id> should be used" 
  echo "    -z, --zookeeper        Comma-separated list of ZK servers with which the brokers"
  echo "                           are registered.  Example: zookeeper1:2181,zookeeper2:2181"
  echo "    -h, --help             Print this help message and exit."
  echo
  echo "Example"
  echo "-------"
  echo
  echo "The following example prints a partition reassignment JSON snippet to STDOUT that scales a topic"
  echo "(from 3 to 4 replicas),brokers randomly selected from the ID range 0,1,2,3,4,5,6,7,8"
  echo
  echo "    $ $MYSELF --topic test-topic --replication 4 --first-broker-id 0 --last-broker-id 8 --zookeeper zookeeper1:2181"
  echo
  # echo "Use cases include:"
  # echo "------------------"
  # echo "  1. Safely restarting a broker while minimizing risk of data loss."
  # echo "  2. Replacing a broker."
  # echo "  3. Preparing a broker for maintenance."
  # echo
  echo "Detailed description"
  echo "--------------------"
  echo "Generates a Kafka partition reassignment JSON snippet to STDOUT"
  echo "to replicate a topic to different, randomly selected broker IDs."
  echo
  echo "This JSON snippet can be saved to a file and then be used as an argument for:"
  echo
  echo "    $ kafka-reassign-partitions.sh --reassignment-json-file my.json"
  echo
  echo "Further information"
  echo "-------------------"
  echo "- http://kafka.apache.org/documentation.html#basic_ops_cluster_expansion"
  echo "- https://cwiki.apache.org/confluence/display/KAFKA/Replication+tools#Replicationtools-6.ReassignPartitionsTool"
}

if [[ $# -eq 0 ]]; then
  print_usage
  exit 97
fi

while [[ $# -gt 0 ]]; do
  case "$1" in
    -t|--topic)
      shift
      declare -r TOPIC="$1"
      shift
      ;;
    -x|--exclude)
      shift
      EXCLUDE_LIST="${EXCLUDE_LIST},$1"
      shift
      ;;
    -r|--replication)
      shift
      declare -r REPLICATION="$1"
      shift
      ;;
    -z|--zookeeper)
      shift
      declare -r ZOOKEEPER_CONNECT="$1"
      shift
      ;;
    -f|--first-broker-id)
      shift
      declare -r KAFKA_FIRST_BROKER_ID="$1"
      shift
      ;;
    -l|--last-broker-id)
      shift
      declare -r KAFKA_LAST_BROKER_ID="$1"
      shift
      ;;
    -p|--kafka-bin-path)
      shift
      declare -r KAFKA_BINARY_PATH="$1"
      shift
      ;;  
    -h|--help)
      print_usage
      exit 98
      ;;
    *)
      echo "ERROR: Unexpected option ${1}"
      echo
      print_usage
      exit 99
      ;;
  esac
done


# Input validation
if [ -z "$TOPIC" ]; then
  echo "ERROR: You must set the parameter --topic"
  exit 80
fi

if [ -z "$REPLICATION" ]; then
  echo "ERROR: You must set the parameter --replication"
  exit 80
fi


if [ -z "$ZOOKEEPER_CONNECT" ]; then
  echo "ERROR: You must set the parameter --zookeeper"
  exit 81
fi

if [ -z "$KAFKA_FIRST_BROKER_ID" ]; then
  echo "ERROR: You must set the parameter --first-broker-id"
  exit 83
fi

if [ -z "$KAFKA_LAST_BROKER_ID" ]; then
  echo "ERROR: You must set the parameter --last-broker-id"
  exit 84
fi

# Remove leading comma, if any.
EXCLUDE_LIST=${EXCLUDE_LIST#","}


###############################################################################
### DEPENDENCIES
###############################################################################

declare -r KAFKA_TOPICS_SCRIPT_NAME_APACHE="kafka-topics.sh"
declare -r KAFKA_TOPICS_SCRIPT_NAME_CONFLUENT="kafka-topics"
if [ -z "$KAFKA_BINARY_PATH" ];then
    declare -r FALLBACK_PATH="/opt/kafka_2.10-0.8.2.1/bin"
else
    declare -r FALLBACK_PATH="$KAFKA_BINARY_PATH"
fi

which "$KAFKA_TOPICS_SCRIPT_NAME_CONFLUENT" &>/dev/null
if [ $? -ne 0 ]; then
  which "$KAFKA_TOPICS_SCRIPT_NAME_APACHE" &>/dev/null
  if [ $? -ne 0 ]; then
    declare -r FALLBACK_BIN="$FALLBACK_PATH/$KAFKA_TOPICS_SCRIPT_NAME_APACHE"
    which "$FALLBACK_BIN" &>/dev/null
    if [ $? -ne 0 ]; then
      echo "ERROR: kafka-topics CLI tool (ships with Kafka) not found in PATH."
      exit 70
    else
      declare -r KAFKA_TOPICS_BIN="$FALLBACK_BIN"
    fi
  else
    declare -r KAFKA_TOPICS_BIN="$KAFKA_TOPICS_SCRIPT_NAME_APACHE"
  fi
else
  declare -r KAFKA_TOPICS_BIN="$KAFKA_TOPICS_SCRIPT_NAME_CONFLUENT"
fi


###############################################################################
### MISC CONFIGURATION - DO NOT TOUCH UNLESS YOU KNOW WHAT YOU ARE DOING
###############################################################################

declare -r OLD_IFS="$IFS"


###############################################################################
### UTILITY FUNCTIONS
###############################################################################

# Checks whether an array (first param) contains an element (second param).
# Returns 0 if the array contains the element, and 1 if it does not.
#
# Usage: array_contains myArray myElement
function array_contains {
  local array="$1[@]"
  local seeking=$2
  local in=1
  for element in "${!array}"; do
    if [[ $element == $seeking ]]; then
      in=0
      break
    fi
  done
  return $in
}

# Randomly selects a broker ID in the range specified by
# KAFKA_FIRST_BROKER_ID (including) and KAFKA_LAST_BROKER_ID (including).
#
# Usage: random_broker  => may return e.g. "6"
function random_broker {
  shuf -i ${KAFKA_FIRST_BROKER_ID}-${KAFKA_LAST_BROKER_ID} -n 1
}

# Randomly selects, from the list of available brokers (range specified by
# KAFKA_FIRST_BROKER_ID and KAFKA_LAST_BROKER_ID), a broker ID that is not
# already listed in the provided brokers (first param).
#
# Usage: other_broker "1,4,6"  => may return e.g. "2"
#
# Note: Do NOT put spaces in the string.  "1,2" is ok, "1, 2" is not.
function other_broker {
  local brokers_string=$1
  local all_brokers_string=`seq -s "," ${KAFKA_FIRST_BROKER_ID} ${KAFKA_LAST_BROKER_ID}`
  if [ ${#brokers_sttring} -ge ${#all_brokers_string} ]; then
    local no_other_broker_available=""
    echo $no_other_broker_available
  else
    IFS=$',' read -a brokers <<< "$brokers_string"
    local new_broker=`random_broker`
    while array_contains brokers $new_broker; do
      new_broker=`random_broker`
    done
    echo $new_broker
  fi
}

# Returns a list of broker IDs by removing the provided broker ID (second param)
# from the provided list of original broker IDs (first param).  If the original
# broker list does not contain the provided broker, the list is returned as is.
#
# The list of broker IDs must be a comma-separated list of numbers, e.g. "1,2".
#
# Usage: all_but_broker "1,2,3" "3"  => returns "1,2"
#
# Note: Do NOT put spaces in the string.  "1,2" is ok, "1, 2" is not.
function all_but_broker {
  local brokers_string=$1
  local broker=$2
  IFS=$',' read -a brokers <<< "$brokers_string"
  local new_brokers=""
  for curr_broker in "${brokers[@]}"; do
    if [ "$curr_broker" != "$broker" ]; then
      new_brokers="$new_brokers,$curr_broker"
    fi
  done
  # Remove leading comma, if any.
  new_brokers=${new_brokers#","}
  echo $new_brokers
}

# Returns a list of broker IDs by removing the provided exclude_list (second param)
# from the provided list of original broker IDs (first param).  If the original
# broker list does not contain the provided brokers, the list is returned as is.
#
# The list of broker IDs must be a comma-separated list of numbers, e.g. "1,2".
#
# Usage: all_but_broker "1,2,3,4" "3,4"  => returns "1,2"
#
# Note: Do NOT put spaces in the string.  "1,2" is ok, "1, 2" is not.
function exclude_brokers {
  local brokers_string=$1
  local exclude_string=$2
  IFS=$',' read -a brokers <<< "$exclude_string"
  local new_brokers=""
  for ex_broker in "${brokers[@]}"; do
    new_brokers=`all_but_broker $brokers_string $ex_broker`  
  done
  # Remove leading comma, if any.
  new_brokers=${new_brokers#","}
  echo $new_brokers
}


# Returns a list of broker IDs based on a provided list of broker IDs (first
# param), where the provided broker ID (second param) is replaced by a
# randomly selected broker ID that is not already in the original list.
#
# Usage: replace_broker "1,2,3" "2"  => may return e.g. "1,3,4"
#
# Note: Do NOT put spaces in the string.  "1,2" is ok, "1, 2" is not.
function replace_broker {
  local brokers_string="$1"
  local broker=$2
  local remaining_brokers=`all_but_broker $brokers_string $broker`
  local replacement_broker=`other_broker $brokers_string,$EXCLUDE_LIST`
  new_brokers="$remaining_brokers,$replacement_broker"
  # Remove leading comma, if any.
  new_brokers=${new_brokers#","}
  # Remove trailing comma, if any.
  new_brokers=${new_brokers%","}
  echo $new_brokers
}

# Returns a list of broker IDs based on a provided list of broker IDs (first
# param), where the provided broker ID (second param) is replaced by a
# randomly selected broker ID that is not already in the original list.
#
# Usage: replace_broker "1,2,3" "2"  => may return e.g. "1,3,4"
#
# Note: Do NOT put spaces in the string.  "1,2" is ok, "1, 2" is not.
function replace_broker_with {
  local brokers_string=$1
  local broker=$2
  local replacement_broker=$3
  local remaining_brokers=`all_but_broker $brokers_string $broker`
  new_brokers="$remaining_brokers,$replacement_broker"
  # Remove leading comma, if any.
  new_brokers=${new_brokers#","}
  # Remove trailing comma, if any.
  new_brokers=${new_brokers%","}
  echo $new_brokers
}

function exclude_brokers2 {
  echo "$1" | tr ',' '\n' | grep -Eiv `echo "$2" | tr ',' '|'` | tr '\n' ',' | sed 's/,$//g'
}

function scale {
  # local 1=$1
  # local 2=$2
  echo "2=$2" >&2

  local to_be_excluded=`echo "${EXCLUDE_LIST},$1" | sed 's/^,//g' | sed 's/,$//g'`
  local range=`seq -s "," $KAFKA_FIRST_BROKER_ID $KAFKA_LAST_BROKER_ID`
  
  local broker_range=`exclude_brokers2 $range $to_be_excluded`
  broker_range=`echo "$1,${broker_range}" | sed 's/^,//g' | sed 's/,$//g'` 

  local new_partitions=(`echo $broker_range|tr ',' ' '`)
  new_Paritions=("${new_partitions[@]:0:$2}")
  
  if [ $2 -lt 0 ] || [ $2 -gt ${#new_partitions} ];then
    echo $replicast
    return 1
  fi
  
  echo $new_Paritions |  tr '\n' ',' | sed 's/^,//g' | sed 's/,$//g'
}

###############################################################################
### MAIN
###############################################################################

# "Header" of JSON file for Kafka partition reassignment
json="{\n"
json="$json  \"partitions\": [\n"

# Actual partition reassignments
for partition in `$KAFKA_TOPICS_BIN --zookeeper $ZOOKEEPER_CONNECT --describe --topic $TOPIC | awk '{ print $4"#"$8 }'`; do
  # Note: We use '#' as field separator in awk (see above) and here
  # because it is not a valid character for a Kafka topic name.
  IFS=$'#' read -a array <<< "$partition"
  partition="${array[0]}" # e.g. "4"
  replicas="${array[1]}"  # e.g. "0,8"  (= comma-separated list of broker IDs)
  
  echo "in main=$REPLICATION"
  $new_replicas=`scale $replicas $REPLICATION`
  # [[ -z "$REPLACE_ID" ]] && new_replicas=`replace_broker $replicas $BROKER` || new_replicas=`replace_broker_with $replicas $BROKER $REPLACE_ID`
  # if [ -z "$new_replicas" ]; then
  #   echo "ERROR: Cannot find any replacement broker.  Maybe you have only a single broker in your cluster?"
  #   exit 60
  # fi
  json="$json    {\"topic\": \"${TOPIC}\", \"partition\": ${partition}, \"replicas\": [${new_replicas}] },\n"
done

# Remove tailing comma, if any.
json=${json%",\n"}
json="${json}\n"

# "Footer" of JSON file
json="$json  ],\n"
json="$json  \"version\": 1\n"
json="${json}}\n"

# Print JSON to STDOUT
echo -e $json


###############################################################################
### CLEANUP
###############################################################################

IFS="$OLD_IFS"