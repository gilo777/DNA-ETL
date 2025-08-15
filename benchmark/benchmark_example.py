#!/usr/bin/env python3
"""
Simple command-line script for benchmarking the genetic data pipeline.
Always generates synthetic data, runs performance tests, and cleans up automatically.

Usage:
    python benchmark_example.py          # Standard test [3, 5, 8]
    python benchmark_example.py -q       # Quick test [2, 3]
    python benchmark_example.py -m       # Medium test [5, 10, 20]
    python benchmark_example.py -l       # Large test [10, 20, 50]
    python benchmark_example.py -xl      # Extra large test [20, 50, 100]
"""

import sys
from SyntheticDataGenerator import SyntheticDataGenerator
from PerformanceBenchmark import PerformanceBenchmark


def run_benchmark(test_sizes, test_name=""):
    """
    Single command that runs complete benchmark with automatic cleanup.
    Perfect for quick testing and CI/CD pipelines.
    """
    print("🧬 Genetic Data Pipeline Performance Benchmark")
    print("=" * 55)
    print(f"🔄 Running {test_name} benchmark with cleanup...")

    print(f"\n📊 Testing with datasets of sizes: {test_sizes}")
    print(f"🗑️  Auto-cleanup: ENABLED")
    print(f"⏱️  Estimated time: {estimate_time(test_sizes)} minutes")
    print("-" * 55)

    # Create benchmark with auto-cleanup (main.py is in parent directory)
    benchmark = PerformanceBenchmark(main_script_path="../main.py", auto_cleanup=True)

    try:
        # Run comprehensive benchmark
        benchmark.run_comprehensive_benchmark(test_sizes)

        # Save results with test name
        results_file = f"benchmark_results_{test_name.lower().replace(' ', '_')}.txt" if test_name else "benchmark_results.txt"
        benchmark.save_detailed_results(results_file)

        print(f"\n✅ Benchmark completed successfully!")
        print(f"📄 Results saved to: {results_file}")
        print(f"🗑️  All test data cleaned up automatically")

        return True

    except Exception as e:
        print(f"\n❌ Benchmark failed: {e}")
        print(f"🗑️  Attempting cleanup...")

        # Try to cleanup even if benchmark failed
        try:
            benchmark.cleanup_test_data()
            print(f"✅ Cleanup completed")
        except:
            print(f"⚠️  Manual cleanup may be required")

        return False


def estimate_time(test_sizes):
    """Estimate benchmark time based on dataset sizes."""
    total_files = sum(test_sizes)
    # Rough estimate: 0.1 seconds per file for all 3 modes
    estimated_seconds = total_files * 0.3
    return round(estimated_seconds / 60, 1)


def run_quick_test():
    """Quick test with small datasets."""
    return run_benchmark([2, 3], "Quick")


def run_medium_test():
    """Medium test with moderate datasets."""
    return run_benchmark([5, 10, 20], "Medium")


def run_large_test():
    """Large test to see concurrent processing advantages."""
    print("⚠️  Large dataset test - this may take 5-10 minutes!")
    proceed = input("Continue? (y/N): ").strip().lower()
    if proceed != 'y':
        print("Test cancelled.")
        return False

    return run_benchmark([10, 20, 50], "Large")


def run_extra_large_test():
    """Extra large test for serious performance analysis."""
    print("🚨 EXTRA LARGE dataset test - this may take 15-30 minutes!")
    print("This will test with up to 100 participants per dataset.")
    proceed = input("Are you sure you want to continue? (y/N): ").strip().lower()
    if proceed != 'y':
        print("Test cancelled.")
        return False

    return run_benchmark([20, 50, 100], "Extra Large")


def run_scaling_analysis():
    """Run a comprehensive scaling analysis across different sizes."""
    print("📈 SCALING ANALYSIS - Complete performance curve")
    print("This will test: [2, 5, 10, 20, 30, 50] - may take 20+ minutes!")
    proceed = input("Continue with full scaling analysis? (y/N): ").strip().lower()
    if proceed != 'y':
        print("Analysis cancelled.")
        return False

    return run_benchmark([2, 5, 10, 20, 30, 50], "Scaling Analysis")


def main():
    """
    Main entry point - runs automated benchmark with cleanup.
    """
    # Check for command line arguments
    if len(sys.argv) > 1:
        arg = sys.argv[1].lower()

        if arg in ['--quick', '-q']:
            success = run_quick_test()
        elif arg in ['--medium', '-m']:
            success = run_medium_test()
        elif arg in ['--large', '-l']:
            success = run_large_test()
        elif arg in ['--extra-large', '-xl']:
            success = run_extra_large_test()
        elif arg in ['--scaling', '-s']:
            success = run_scaling_analysis()
        elif arg in ['--help', '-h']:
            print("🧬 Genetic Data Pipeline Benchmark")
            print("\nUsage:")
            print("  python benchmark_example.py          # Standard test [3, 5, 8]")
            print("  python benchmark_example.py -q       # Quick test [2, 3]")
            print("  python benchmark_example.py -m       # Medium test [5, 10, 20]")
            print("  python benchmark_example.py -l       # Large test [10, 20, 50]")
            print("  python benchmark_example.py -xl      # Extra large [20, 50, 100]")
            print("  python benchmark_example.py -s       # Scaling analysis [2,5,10,20,30,50]")
            print("  python benchmark_example.py -h       # Show this help")
            print("\nFeatures:")
            print("  • Generates synthetic genetic data")
            print("  • Tests single file, sequential, and concurrent processing")
            print("  • Automatically cleans up all test data")
            print("  • Saves detailed performance results")
            print("\nPerformance Expectations:")
            print("  • Small datasets: Sequential often fastest")
            print("  • Large datasets: Concurrent should outperform")
            print("  • Single file: Always has startup overhead")
            return
        else:
            print(f"❌ Unknown option: {arg}")
            print("Use -h for help")
            return
    else:
        # Run the standard benchmark
        success = run_benchmark([3, 5, 8], "Standard")

    # Exit with appropriate code
    sys.exit(0 if success else 1)


# Keep the other examples as separate functions that can be called if needed
def example_data_generation():
    """
    Simple example of data generation with immediate cleanup.
    """
    print("🔬 Data Generation Example")
    print("-" * 30)

    generator = SyntheticDataGenerator("./ExampleTest")
    configs = generator.generate_dataset(3)

    # Show what was created
    size_info = generator.get_data_size_info()
    print(f"✓ Generated {len(configs)} configs ({size_info['total_size_mb']} MB)")

    # Clean up immediately
    generator.cleanup_generated_data()
    print("✓ Cleaned up automatically")


def example_benchmark_only():
    """
    Example that only runs benchmarking (no extra examples).
    """
    print("📊 Benchmark Only Example")
    print("-" * 30)

    benchmark = PerformanceBenchmark(main_script_path="../main.py", auto_cleanup=True)
    benchmark.run_comprehensive_benchmark([2, 4])
    print("✓ Benchmark completed with auto-cleanup")


if __name__ == "__main__":
    main()