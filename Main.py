import argparse
import os
import sys
from Pipeline.ETLOrchestrator import ETLOrchestrator

def main():
    parser = argparse.ArgumentParser(
        description='Run the pipeline orchestrator with a JSON configuration file'
    )
    parser.add_argument(
        'config_path',
        help='Path to the JSON configuration file for the pipeline'
    )
    args = parser.parse_args()

    orchestrator = ETLOrchestrator()
    if os.path.isfile(args.config_path):
        success = process_single_file(orchestrator, args.config_path)
        sys.exit(0 if success else 1)
    elif os.path.isdir(args.config_path):
        success = process_directory(orchestrator, args.config_path)
        sys.exit(0 if success else 1)
    else:
        print(f"\n ✗ Error: '{args.config_path}' is neither a regular file nor a directory.")
        sys.exit(1)


def process_single_file(orchestrator, file_path):
    result = orchestrator.orchestrate(file_path)
    if result[0] == 0:
        print(f"\n ✓ Successfully processed {file_path}: \n {result[1]}")
    elif result[0] == 1:
        print(f"\n ✗ Failed to process {file_path}: \n {result[1]}")
    return True

def process_directory(orchestrator, directory_path):
    all_items = os.listdir(directory_path)
    json_files = [item for item in all_items
                  if item.lower().endswith('.json')]
    if not json_files:
        print(f"\n ✗ No JSON files found in directory '{directory_path}'")
        return False
    successful_count = 0
    total_count = len(json_files)
    for json_file in json_files:
        full_path = os.path.join(directory_path, json_file)
        if process_single_file(orchestrator, full_path):
            successful_count += 1
    return successful_count == total_count


if __name__ == "__main__":
    main()