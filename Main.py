import argparse
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from Pipeline.ETLOrchestrator import ETLOrchestrator


def main():
    """
    Main entry point for the ETL pipeline orchestrator.
    :return:
        None: Function exits the program with appropriate exit code
    """
    parser = argparse.ArgumentParser(
        description="Run the pipeline orchestrator with a JSON configuration file"
    )
    parser.add_argument(
        "config_path", help="Path to the JSON configuration file for the pipeline"
    )
    parser.add_argument(
        "mode",
        nargs="?",
        choices=["concurrent", "sequential"],
        default="sequential",
        help="Processing mode: c for concurrent, s for sequential (only for directories)",
    )
    parser.add_argument(
        "num_threads",
        nargs="?",
        default=None,
        type=int,
        help="Number of threads to use for parallel processing",
    )
    args = parser.parse_args()

    if os.path.isfile(args.config_path):
        orchestrator = ETLOrchestrator()
        success = process_single_file(orchestrator, args.config_path)
        sys.exit(0 if success else 1)
    elif os.path.isdir(args.config_path) and (args.mode == "sequential"):
        orchestrator = ETLOrchestrator()
        success = process_directory(orchestrator, args.config_path)
        sys.exit(0 if success else 1)
    elif os.path.isdir(args.config_path) and args.mode == "concurrent":
        success = process_directory_concurrent(args.config_path, args.num_threads)
        sys.exit(0 if success else 1)
    else:
        print(
            f"\n ✗ Error: '{args.config_path}' is neither a regular file nor a directory."
        )
        sys.exit(1)


def process_single_file(orchestrator : ETLOrchestrator, file_path : str) -> bool:
    """
    Process a single JSON configuration file through the ETL orchestrator.

    :param orchestrator: The ETL orchestrator instance to use for processing
    :param file_path: Path to the JSON configuration file to process
    :return:
        bool: True if successful, False otherwise
    """
    result = orchestrator.orchestrate(file_path)
    if result[0] == 0:
        print(f"\n ✓ Successfully processed {file_path}: \n {result[1]}")
        return True
    elif result[0] == 1:
        print(f"\n ✗ Failed to process {file_path}: \n {result[1]}")
        return False


def process_directory(orchestrator : ETLOrchestrator, directory_path : str) -> bool:
    """
    Process all JSON files in a directory sequentially using a single ETL orchestrator instance.

    :param orchestrator: The ETL orchestrator instance to use for processing
    :param directory_path: Path to directory containing JSON configuration files
    :return:
        True if all files processed successfully, False if any file failed or no JSON files found

    Note:
        Processing stops on first failure but continues to process remaining files.
    """
    all_items = os.listdir(directory_path)
    json_files = [item for item in all_items if item.lower().endswith(".json")]
    if not json_files:
        print(f"\n ✗ No JSON files found in directory '{directory_path}'")
        return False
    successful_files = []
    failed_files = []
    for json_file in json_files:
        full_path = os.path.join(directory_path, json_file)
        result = orchestrator.orchestrate(full_path)
        if result[0] == 0:
            successful_files.append(json_file)
        else:
            failed_files.append((json_file, result[1]))
    print_summary_report(directory_path, successful_files, failed_files, "Sequential")
    return len(failed_files) == 0


def process_directory_concurrent(directory_path : str, num_threads : int) -> bool:
    """
    Process all JSON files in a directory concurrently using multiple threads.

    :param directory_path: Path to directory containing JSON configuration files
    :param num_threads: Number of worker threads to use for parallel processing

    :return:
        bool: True if all files processed successfully, False if any file failed or no JSON files found

    Note:
        - Each worker thread creates its own ETLOrchestrator instance to avoid thread safety issues
        - Uses ThreadPoolExecutor for managed thread pool execution
    """
    all_items = os.listdir(directory_path)
    json_files = [item for item in all_items if item.lower().endswith(".json")]
    if not json_files:
        print(f"\n ✗ No JSON files found in directory '{directory_path}'")
        return False

    successful_files = []
    failed_files = []

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        future_to_file = {
            executor.submit(process_file_worker, directory_path, json_file): json_file
            for json_file in json_files
        }

        for future in as_completed(future_to_file):
            file_path, status_code, message = future.result()
            file_name = os.path.basename(file_path)

            if status_code == 0:
                successful_files.append(file_name)
            else:
                failed_files.append((file_name, message))

    print_summary_report(directory_path, successful_files, failed_files, "Concurrent")

    return len(failed_files) == 0


def print_summary_report(directory_path : str, successful_files : int, failed_files : int, mode : str) -> None:
    """
    Print a formatted summary report of file processing results.

    :param directory_path: Path to the directory that was processed
    :param successful_files: List of successfully processed file names
    :param failed_files: List of tuples containing (file_name, error_message) for failed files
    :param mode: Processing mode used ('Sequential' or 'Concurrent')
    """
    total_files = len(successful_files) + len(failed_files)
    success_count = len(successful_files)
    failure_count = len(failed_files)
    success_rate = (success_count / total_files) * 100 if total_files > 0 else 0

    print(f"PROCESSING SUMMARY - {mode} Mode")
    print(f"{'=' * 60}")
    print(f"Directory: {directory_path}")
    print(f"Total files processed: {total_files}")
    print(f"✓ Successful: {success_count}")
    print(f"✗ Failed: {failure_count}")
    print(f"Success rate: {success_rate:.1f}%")

    if failed_files:
        print(f"\nFailed files:")
        for file_name, error_msg in failed_files:
            print(f"  • {file_name}: {error_msg}")


def process_file_worker(directory_path : str, json_file : str) -> tuple[str, int, str]:
    """
    Worker function for processing a single JSON file in a thread-safe manner.

    :param directory_path: Path to the directory containing the JSON file
    :param json_file: Name of the JSON file to process (not the full path)
    :return:
        tuple[str, int, str]: A tuple containing:
            - str: Full path to the processed file
            - int: Status code (0 for success, non-zero for failure)
            - str: Result message or error description
    """
    orchestrator = ETLOrchestrator()
    full_path = os.path.join(directory_path, json_file)
    result = orchestrator.orchestrate(full_path)
    return full_path, result[0], result[1]


if __name__ == "__main__":
    main()
