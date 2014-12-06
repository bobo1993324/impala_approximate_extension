#!/bin/bash

SOURCE="/usr/bin/impala-shell"
while [ -h "$SOURCE" ]
do
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE"
  BIN_DIR="$( cd -P "$( dirname "$SOURCE"  )" && pwd )"
done
BIN_DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
LIB_DIR=$BIN_DIR/../lib

sed s/impala_shell.py/blinkdb_shell.py/ < $BIN_DIR/impala-shell > $BIN_DIR/blinkdb-shell
cp shell/blinkdb_shell.py $LIB_DIR/impala-shell
ln -s $BIN_DIR/blinkdb-shell /usr/bin
