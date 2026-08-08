#!/bin/bash

# run.sh - Helper script to compile and run MapReduce jobs for Assignment 3

# Make sure Hadoop is in PATH
if ! command -v hadoop &> /dev/null
then
    echo "Hadoop could not be found. Please ensure it is in your PATH."
    exit 1
fi

if [ -z "$1" ]; then
    echo "Usage: ./run.sh <ProgramNumber>"
    echo "Programs available:"
    echo "  1 - CountryTransactions"
    echo "  2 - CountrySales"
    echo "  3 - TopProducts"
    echo "  4 - CancelledTransactions"
    echo "  5 - CustomerOrders"
    exit 1
fi

PROGRAM_NUM=$1
CLASS_NAME=""

case $PROGRAM_NUM in
    1) CLASS_NAME="CountryTransactions" ;;
    2) CLASS_NAME="CountrySales" ;;
    3) CLASS_NAME="TopProducts" ;;
    4) CLASS_NAME="CancelledTransactions" ;;
    5) CLASS_NAME="CustomerOrders" ;;
    *) echo "Invalid Program Number!"; exit 1 ;;
esac

echo "======================================"
echo " Running: $CLASS_NAME"
echo "======================================"

# Paths
SRC_DIR="src"
BUILD_DIR="build"
INPUT_DATA="inputs/OnlineRetail.csv"
HDFS_INPUT_DIR="/input_assg3"
HDFS_OUTPUT_DIR="/output_assg3"

# 1. Clean build directory
rm -rf $BUILD_DIR
mkdir -p $BUILD_DIR

# 2. Compile Java Class
echo "[1/4] Compiling $CLASS_NAME.java..."
javac -classpath $(hadoop classpath) -d $BUILD_DIR $SRC_DIR/$CLASS_NAME.java

if [ $? -ne 0 ]; then
    echo "Compilation failed!"
    exit 1
fi

# 3. Create JAR file
echo "[2/4] Creating job.jar..."
jar -cvf job.jar -C $BUILD_DIR/ . > /dev/null

# 4. Prepare HDFS Input
echo "[3/4] Preparing HDFS Input Directory..."
hadoop fs -rm -r -f $HDFS_INPUT_DIR 2> /dev/null
hadoop fs -mkdir -p $HDFS_INPUT_DIR
hadoop fs -put $INPUT_DATA $HDFS_INPUT_DIR/

# 5. Clean HDFS Output
hadoop fs -rm -r -f $HDFS_OUTPUT_DIR 2> /dev/null

# 6. Run MapReduce Job
echo "[4/4] Executing MapReduce Job on Hadoop..."
hadoop jar job.jar $CLASS_NAME $HDFS_INPUT_DIR $HDFS_OUTPUT_DIR

# 7. Display Output
echo "======================================"
echo " JOB COMPLETED! Output Preview:"
echo "======================================"
hadoop fs -cat $HDFS_OUTPUT_DIR/part-r-00000 | head -n 20
