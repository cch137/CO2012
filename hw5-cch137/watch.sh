#!/bin/bash

current_pid=0
last_modified_file=""
last_modified_time_ms=0

DEBOUNCE_INTERVAL_MS=1500      # Debounce interval in milliseconds to prevent duplicate triggers
WATCH_PATTERN='^(\./)?([^.][^/]*/)*[^.][^/]*\.(c|h)$'     # Updated pattern to ignore hidden directories
OUTPUT_EXECUTABLE="main"       # Name of the output executable
ENTRY_POINT="main"             # Default entry point

# Function to display the selection menu with arrow keys
function select_entry_point {
    local options=("main" "test")
    local selected=0

    while true; do
        clear
        echo "Select the entry point for the program:"

        for i in "${!options[@]}"; do
            if [ "$i" -eq "$selected" ]; then
                echo -e "\e[1;32m> ${options[$i]}\e[0m"  # Highlight selected option
            else
                echo "  ${options[$i]}"
            fi
        done

        # Read user input
        read -rsn1 key

        case "$key" in
            $'\x1b')
                read -rsn2 key  # Read additional characters for arrow keys
                case "$key" in
                    "[A") # Up arrow
                        selected=$(( (selected - 1 + ${#options[@]}) % ${#options[@]} ))
                        ;;
                    "[B") # Down arrow
                        selected=$(( (selected + 1) % ${#options[@]} ))
                        ;;
                esac
                ;;
            "")  # Enter key
                break
                ;;
            *)  # Ignore all other keys, including Space
                continue
                ;;
        esac
    done

    ENTRY_POINT="${options[$selected]}"
    OUTPUT_EXECUTABLE="$ENTRY_POINT"

    echo -e "\n\e[44;37mEntry point selected: $ENTRY_POINT\e[0m\n"
}

function compile_and_run {
    # Filter .c files based on the selected entry point
    if [ "$ENTRY_POINT" == "main" ]; then
        c_files=$(find . -type f -name "*.c" ! -path "*/\.*/*" ! -name ".*" ! -name "test.c" -print | tr '\n' ' ')
    else
        c_files=$(find . -type f -name "*.c" ! -path "*/\.*/*" ! -name ".*" ! -name "main.c" -print | tr '\n' ' ')
    fi

    compile_command="gcc -o $OUTPUT_EXECUTABLE $c_files"  # Build command

    # Terminate the previous process if it's still running
    if [ $current_pid -ne 0 ] && kill -0 $current_pid 2>/dev/null; then
        kill $current_pid
        wait $current_pid 2>/dev/null
    fi

    # Compile the program
    eval $compile_command
    if [ $? -eq 0 ]; then
        # Run the program in the background, redirecting stdin
        ./$OUTPUT_EXECUTABLE < /dev/tty &
        current_pid=$!
    else
        echo -e "\n\e[41;37mCompilation failed. Skipping execution.\e[0m\n"
    fi
}

function monitor_files {
    compile_and_run

    inotifywait -m -r -e modify --format '%w%f' --include "$WATCH_PATTERN" --quiet . |
    while read -r modified_file; do
        # Ignore modifications in hidden directories
        if [[ "$modified_file" == */.* ]]; then
            continue
        fi

        current_time_ms=$(($(date +%s%N) / 1000000))

        # Skip if the same file change is detected within the debounce interval
        if [[ "$modified_file" == "$last_modified_file" && $((current_time_ms - last_modified_time_ms)) -lt $DEBOUNCE_INTERVAL_MS ]]; then
            continue
        fi

        last_modified_file="$modified_file"
        last_modified_time_ms=$current_time_ms

        echo -e "\n\e[44;37mFile $modified_file changed, recompiling and running...\e[0m\n"
        compile_and_run
    done
}

# Handle exit to clean up background processes
trap "kill $current_pid 2>/dev/null" EXIT

# Select the entry point before monitoring files
select_entry_point
monitor_files
