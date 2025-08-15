import os
import time
import subprocess
import statistics
import shutil
from pathlib import Path
from typing import List, Dict, Tuple
from SyntheticDataGenerator import SyntheticDataGenerator


class PerformanceBenchmark:
    """
    Measures and compares performance of different processing modes in the ETL pipeline.

    This class helps analyze the efficiency of:
    - Single file processing
    - Directory sequential processing
    - Directory concurrent processing

    It provides detailed timing analysis and comparison metrics for educational purposes.
    """

    def __init__(self, main_script_path: str = "../main.py", auto_cleanup: bool = False):
        """
        Initialize the performance benchmark.

        :param main_script_path: Path to the main.py script to benchmark (default: "main.py")
        :param auto_cleanup: Whether to automatically clean up test data after benchmarking
        """
        self.main_script_path = main_script_path
        self.auto_cleanup = auto_cleanup
        self.test_data_generators = []  # Track generators for cleanup
        self.results = {
            'single_file': [],
            'directory_sequential': [],
            'directory_concurrent': []
        }

    def run_comprehensive_benchmark(self, test_sizes: List[int] = None, cleanup_after: bool = None) -> None:
        """
        Runs a comprehensive benchmark across different dataset sizes.

        :param test_sizes: List of participant counts to test (default: [5, 10, 20])
        :param cleanup_after: Override auto_cleanup setting for this run
        """
        if test_sizes is None:
            test_sizes = [5, 10, 20]

        should_cleanup = cleanup_after if cleanup_after is not None else self.auto_cleanup

        print("🧪 Starting Comprehensive Performance Benchmark")
        print("=" * 60)

        try:
            for size in test_sizes:
                print(f"\n📊 Testing with {size} participants")
                print("-" * 40)

                # Generate synthetic data for this test
                generator = SyntheticDataGenerator(f"./BenchmarkData_{size}")
                self.test_data_generators.append(generator)
                input_configs = generator.generate_dataset(size)

                # Run benchmarks for all three modes
                self._benchmark_single_file_mode(input_configs, size)
                self._benchmark_directory_sequential_mode(input_configs, size)
                self._benchmark_directory_concurrent_mode(input_configs, size)

                print(f"✓ Completed testing for {size} participants")

            # Generate final comparison report
            self._generate_comparison_report(test_sizes)

        finally:
            # Cleanup if requested
            if should_cleanup:
                self.cleanup_test_data()
            else:
                print(f"\n💾 Test data preserved. Use cleanup_test_data() to remove it manually.")
                self._show_cleanup_info()

    def _benchmark_single_file_mode(self, input_configs: List[str], dataset_size: int) -> None:
        """
        Benchmarks single file processing mode.

        :param input_configs: List of input configuration file paths
        :param dataset_size: Number of participants in this test
        """
        print("  📄 Testing: Single file processing...")

        times = []
        successful_runs = 0

        for config_file in input_configs:
            start_time = time.time()

            # Run the pipeline for single file
            success = self._run_pipeline_command([config_file])

            end_time = time.time()
            processing_time = end_time - start_time

            if success:
                times.append(processing_time)
                successful_runs += 1

        # Calculate statistics
        if times:
            avg_time = statistics.mean(times)
            total_time = sum(times)
            throughput = successful_runs / total_time if total_time > 0 else 0

            result = {
                'dataset_size': dataset_size,
                'total_time': total_time,
                'average_time_per_file': avg_time,
                'successful_runs': successful_runs,
                'throughput_files_per_second': throughput,
                'individual_times': times
            }

            self.results['single_file'].append(result)

            print(f"    ✅ Total time: {total_time:.2f}s")
            print(f"    ✅ Avg per file: {avg_time:.2f}s")
            print(f"    ✅ Throughput: {throughput:.2f} files/sec")
        else:
            print("    ❌ No successful runs")

    def _benchmark_directory_sequential_mode(self, input_configs: List[str], dataset_size: int) -> None:
        """
        Benchmarks directory sequential processing mode.

        :param input_configs: List of input configuration file paths
        :param dataset_size: Number of participants in this test
        """
        print("  📁 Testing: Directory sequential processing...")

        # Get the directory containing the input configs
        config_directory = str(Path(input_configs[0]).parent)

        start_time = time.time()

        # Run pipeline for entire directory in sequential mode
        success = self._run_pipeline_command([config_directory, 'sequential'])

        end_time = time.time()
        total_time = end_time - start_time

        if success:
            avg_time_per_file = total_time / len(input_configs)
            throughput = len(input_configs) / total_time if total_time > 0 else 0

            result = {
                'dataset_size': dataset_size,
                'total_time': total_time,
                'average_time_per_file': avg_time_per_file,
                'successful_runs': len(input_configs),
                'throughput_files_per_second': throughput
            }

            self.results['directory_sequential'].append(result)

            print(f"    ✅ Total time: {total_time:.2f}s")
            print(f"    ✅ Avg per file: {avg_time_per_file:.2f}s")
            print(f"    ✅ Throughput: {throughput:.2f} files/sec")
        else:
            print("    ❌ Sequential processing failed")

    def _benchmark_directory_concurrent_mode(self, input_configs: List[str], dataset_size: int) -> None:
        """
        Benchmarks directory concurrent processing mode.

        :param input_configs: List of input configuration file paths
        :param dataset_size: Number of participants in this test
        """
        print("  🚀 Testing: Directory concurrent processing...")

        # Get the directory containing the input configs
        config_directory = str(Path(input_configs[0]).parent)

        start_time = time.time()

        # Run pipeline for entire directory in concurrent mode
        success = self._run_pipeline_command([config_directory, 'concurrent'])

        end_time = time.time()
        total_time = end_time - start_time

        if success:
            avg_time_per_file = total_time / len(input_configs)
            throughput = len(input_configs) / total_time if total_time > 0 else 0

            result = {
                'dataset_size': dataset_size,
                'total_time': total_time,
                'average_time_per_file': avg_time_per_file,
                'successful_runs': len(input_configs),
                'throughput_files_per_second': throughput
            }

            self.results['directory_concurrent'].append(result)

            print(f"    ✅ Total time: {total_time:.2f}s")
            print(f"    ✅ Avg per file: {avg_time_per_file:.2f}s")
            print(f"    ✅ Throughput: {throughput:.2f} files/sec")
        else:
            print("    ❌ Concurrent processing failed")

    def _run_pipeline_command(self, args: List[str]) -> bool:
        """
        Runs the pipeline with given arguments and returns success status.

        :param args: Command line arguments for the pipeline
        :return: True if pipeline completed successfully, False otherwise
        """
        try:
            # Construct the command
            cmd = ['python', self.main_script_path] + args

            # Run the command and capture output
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=300  # 5 minute timeout
            )

            # Only show debug info for failures
            if result.returncode != 0:
                print(f"      ❌ Command failed: {' '.join(cmd)}")
                if result.stderr:
                    print(f"      📤 Error: {result.stderr[:100]}...")

            # Return True if exit code is 0 (success)
            return result.returncode == 0

        except subprocess.TimeoutExpired:
            print("      ⚠️  Pipeline timed out")
            return False
        except FileNotFoundError as e:
            print(f"      ⚠️  File not found: {e}")
            return False
        except Exception as e:
            print(f"      ⚠️  Error running pipeline: {e}")
            return False

    def _generate_comparison_report(self, test_sizes: List[int]) -> None:
        """
        Generates a comprehensive comparison report of all benchmark results.

        :param test_sizes: List of dataset sizes that were tested
        """
        print("\n" + "=" * 80)
        print("📈 PERFORMANCE COMPARISON REPORT")
        print("=" * 80)

        # Performance summary table
        print("\n🏃 SPEED COMPARISON (Total Time)")
        print("-" * 60)
        print(f"{'Dataset Size':<15} {'Single File':<15} {'Sequential':<15} {'Concurrent':<15}")
        print("-" * 60)

        for size in test_sizes:
            single_time = self._get_result_value('single_file', size, 'total_time')
            sequential_time = self._get_result_value('directory_sequential', size, 'total_time')
            concurrent_time = self._get_result_value('directory_concurrent', size, 'total_time')

            print(f"{size:<15} {single_time:<15.2f} {sequential_time:<15.2f} {concurrent_time:<15.2f}")

        # Throughput comparison
        print("\n⚡ THROUGHPUT COMPARISON (Files/Second)")
        print("-" * 60)
        print(f"{'Dataset Size':<15} {'Single File':<15} {'Sequential':<15} {'Concurrent':<15}")
        print("-" * 60)

        for size in test_sizes:
            single_throughput = self._get_result_value('single_file', size, 'throughput_files_per_second')
            sequential_throughput = self._get_result_value('directory_sequential', size, 'throughput_files_per_second')
            concurrent_throughput = self._get_result_value('directory_concurrent', size, 'throughput_files_per_second')

            print(
                f"{size:<15} {single_throughput:<15.3f} {sequential_throughput:<15.3f} {concurrent_throughput:<15.3f}")

        # Analysis and insights
        print("\n🔍 ANALYSIS & INSIGHTS")
        print("-" * 40)

        self._analyze_performance_trends()

        # Recommendations
        print("\n💡 RECOMMENDATIONS")
        print("-" * 40)
        self._generate_recommendations()

    def _get_result_value(self, mode: str, dataset_size: int, metric: str) -> float:
        """
        Helper method to extract specific metric values from results.

        :param mode: Processing mode ('single_file', 'directory_sequential', 'directory_concurrent')
        :param dataset_size: Size of the dataset
        :param metric: Metric name to extract
        :return: Metric value or 0 if not found
        """
        for result in self.results[mode]:
            if result['dataset_size'] == dataset_size:
                return result.get(metric, 0)
        return 0

    def _analyze_performance_trends(self) -> None:
        """
        Analyzes performance trends across different modes and dataset sizes.
        """
        if not (self.results['single_file'] and self.results['directory_sequential'] and self.results[
            'directory_concurrent']):
            print("• Insufficient data for comprehensive analysis")
            return

        # Find dataset sizes where concurrent becomes faster than sequential
        concurrent_advantage_point = None
        for result_concurrent, result_sequential in zip(self.results['directory_concurrent'],
                                                        self.results['directory_sequential']):
            size = result_concurrent['dataset_size']
            concurrent_throughput = result_concurrent['throughput_files_per_second']
            sequential_throughput = result_sequential['throughput_files_per_second']

            if concurrent_throughput > sequential_throughput:
                if concurrent_advantage_point is None:
                    concurrent_advantage_point = size
                print(
                    f"• Concurrent processing becomes advantageous at {size}+ files ({concurrent_throughput:.2f} vs {sequential_throughput:.2f} files/sec)")
                break

        if concurrent_advantage_point is None:
            max_size = max([r['dataset_size'] for r in self.results['directory_concurrent']])
            print(f"• Sequential processing remained fastest up to {max_size} files")
            print(f"• Concurrent processing may become advantageous with even larger datasets")

        # Analyze single file overhead
        if self.results['single_file']:
            largest_single = max(self.results['single_file'], key=lambda x: x['dataset_size'])
            largest_sequential = max(self.results['directory_sequential'], key=lambda x: x['dataset_size'])

            if largest_single['dataset_size'] == largest_sequential['dataset_size']:
                single_throughput = largest_single['throughput_files_per_second']
                sequential_throughput = largest_sequential['throughput_files_per_second']
                overhead_factor = sequential_throughput / single_throughput if single_throughput > 0 else 0

                print(f"• Batch processing is {overhead_factor:.1f}x faster than single file processing")
                print(f"• Single file overhead: significant for {largest_single['dataset_size']} files")

        # Find fastest mode for largest dataset
        largest_dataset = max([r['dataset_size'] for r in self.results['single_file']])

        concurrent_time = self._get_result_value('directory_concurrent', largest_dataset, 'total_time')
        sequential_time = self._get_result_value('directory_sequential', largest_dataset, 'total_time')
        single_time = self._get_result_value('single_file', largest_dataset, 'total_time')

        times = [
            ('Concurrent', concurrent_time),
            ('Sequential', sequential_time),
            ('Single File', single_time)
        ]
        times.sort(key=lambda x: x[1])

        print(f"• For {largest_dataset} files, performance ranking: {', '.join([mode for mode, _ in times])}")

        # Scaling analysis
        if len(self.results['directory_concurrent']) >= 3:
            print(
                f"• Concurrent processing scaling: Tests show performance across {len(self.results['directory_concurrent'])} dataset sizes")
            print(f"• Sequential processing scaling: Consistent performance, predictable timing")

    def _generate_recommendations(self) -> None:
        """
        Generates recommendations based on benchmark results.
        """
        if not (self.results['single_file'] and self.results['directory_sequential'] and self.results[
            'directory_concurrent']):
            print("• Run larger datasets to get comprehensive recommendations")
            return

        # Find the crossover point where concurrent becomes faster
        concurrent_advantage_found = False
        for result_concurrent, result_sequential in zip(self.results['directory_concurrent'],
                                                        self.results['directory_sequential']):
            size = result_concurrent['dataset_size']
            if result_concurrent['throughput_files_per_second'] > result_sequential['throughput_files_per_second']:
                print(f"• Use concurrent processing for datasets with {size}+ files")
                concurrent_advantage_found = True
                break

        if not concurrent_advantage_found:
            max_tested = max([r['dataset_size'] for r in self.results['directory_concurrent']])
            print(f"• Sequential processing was fastest up to {max_tested} files")
            print(f"• Test larger datasets (50+ files) to find concurrent processing advantages")

        # General recommendations
        print("• Use sequential processing for small datasets and when consistency is critical")
        print("• Use single file processing for individual file testing and debugging")

        # System-specific recommendations based on results
        if self.results['directory_concurrent']:
            avg_concurrent_speedup = sum(
                r['throughput_files_per_second'] for r in self.results['directory_concurrent']) / len(
                self.results['directory_concurrent'])
            avg_sequential_speedup = sum(
                r['throughput_files_per_second'] for r in self.results['directory_sequential']) / len(
                self.results['directory_sequential'])

            if avg_concurrent_speedup > avg_sequential_speedup * 1.2:
                print("• Your system shows good concurrent processing performance")
            elif avg_sequential_speedup > avg_concurrent_speedup * 1.2:
                print("• Your system favors sequential processing (possibly I/O bound)")
            else:
                print("• Your system shows balanced performance between concurrent and sequential")

        print("• Consider system resources and memory when choosing processing mode")

    def save_detailed_results(self, filename: str = "benchmark_results.txt") -> None:
        """
        Saves detailed benchmark results to a file.

        :param filename: Name of the file to save results to
        """
        with open(filename, 'w') as f:
            f.write("DETAILED BENCHMARK RESULTS\n")
            f.write("=" * 50 + "\n\n")

            for mode, results in self.results.items():
                f.write(f"{mode.upper().replace('_', ' ')} MODE\n")
                f.write("-" * 30 + "\n")

                for result in results:
                    f.write(f"Dataset Size: {result['dataset_size']}\n")
                    f.write(f"Total Time: {result['total_time']:.3f}s\n")
                    f.write(f"Average Time per File: {result['average_time_per_file']:.3f}s\n")
                    f.write(f"Throughput: {result['throughput_files_per_second']:.3f} files/sec\n")
                    f.write("\n")

                f.write("\n")

        print(f"✓ Detailed results saved to {filename}")

    def cleanup_test_data(self) -> None:
        """
        Removes all generated test data directories.
        """
        if not self.test_data_generators:
            print("No test data to clean up")
            return

        print("\n🗑️  Cleaning up test data...")
        total_cleaned = 0

        for generator in self.test_data_generators:
            if generator.base_output_dir.exists():
                size_info = generator.get_data_size_info()
                generator.cleanup_generated_data()
                total_cleaned += size_info['total_size_mb']

        print(f"✓ Cleanup completed - freed {total_cleaned:.2f} MB")
        self.test_data_generators.clear()

    def _show_cleanup_info(self) -> None:
        """
        Shows information about test data that can be cleaned up.
        """
        if not self.test_data_generators:
            return

        print("\n📁 Test Data Information:")
        print("-" * 30)
        total_size = 0

        for i, generator in enumerate(self.test_data_generators):
            size_info = generator.get_data_size_info()
            total_size += size_info['total_size_mb']
            print(f"  Dataset {i + 1}: {size_info['total_size_mb']} MB at {size_info['base_path']}")

        print(f"  Total: {total_size:.2f} MB")
        print("\n💡 To clean up: benchmark.cleanup_test_data()")


# Example usage for testing
if __name__ == "__main__":
    # Create benchmark instance with auto-cleanup disabled by default
    benchmark = PerformanceBenchmark(auto_cleanup=False)

    # Run comprehensive benchmark with different dataset sizes
    benchmark.run_comprehensive_benchmark([3, 5, 8])

    # Save detailed results
    benchmark.save_detailed_results()

    # Manual cleanup (uncomment if you want to clean up immediately)
    benchmark.cleanup_test_data()