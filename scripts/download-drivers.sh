#!/bin/bash

# Download MySQL JDBC driver if not exists
DRIVER_DIR="./drivers"
DRIVER_FILE="$DRIVER_DIR/mysql-connector-j-8.3.0.jar"
DRIVER_URL="https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.3.0/mysql-connector-j-8.3.0.jar"

mkdir -p "$DRIVER_DIR"

if [ ! -f "$DRIVER_FILE" ]; then
    echo "Downloading MySQL JDBC driver..."
    curl -L -o "$DRIVER_FILE" "$DRIVER_URL"
    echo "MySQL JDBC driver downloaded successfully"
else
    echo "MySQL JDBC driver already exists"
fi
