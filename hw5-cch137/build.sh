#!/bin/bash

WATCH_PATTERN='^(\./)?([^.][^/]*/)*[^.][^/]*\.(c|h)$'     # Updated pattern to ignore hidden directories

OUTPUT_EXECUTABLE="main" # Name of the output executable
c_files=$(find . -type f -name "*.c" ! -path "*/\.*/*" ! -name ".*" ! -name "test.c" -print | tr '\n' ' ')
compile_command="gcc -o $OUTPUT_EXECUTABLE $c_files"  # Build command
eval $compile_command # Compile the program

OUTPUT_EXECUTABLE="test"
c_files=$(find . -type f -name "*.c" ! -path "*/\.*/*" ! -name ".*" ! -name "main.c" -print | tr '\n' ' ')
compile_command="gcc -o $OUTPUT_EXECUTABLE $c_files" 
eval $compile_command  
