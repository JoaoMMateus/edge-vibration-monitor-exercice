#!/usr/bin/env python
"""
CLI script to run the edge vibration streaming processor locally.

Usage:
    python run_processor.py benchmark          # Run throughput benchmark with synthetic data
    python run_processor.py realtime [--duration SEC]  # Run real-time simulation
    python run_processor.py interactive        # Interactive mode with live output

Examples:
    python run_processor.py benchmark
    python run_processor.py benchmark --duration 30 --window-size 2.0
    python run_processor.py realtime --duration 15
    python run_processor.py interactive --sample-rate 500
"""

import argparse
import json
import sys
import time
from typing import Optional, List

from edge.sensor import VibrationSensor, SensorConfig, SensorSample
from edge.processor import (
    StreamingProcessor,
    WindowFeatures,
    benchmark_throughput
)


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Run the edge vibration streaming processor locally",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_processor.py benchmark
  python run_processor.py benchmark --duration 30 --window-size 2.0
  python run_processor.py csv --path data/sample_dataset_small.csv
  python run_processor.py realtime --duration 15
  python run_processor.py interactive --sample-rate 500
        """
    )
    
    # Mode selection (mutually exclusive)
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument(
        "--benchmark", "-b",
        action="store_true",
        help="Run throughput benchmark with synthetic data"
    )
    mode_group.add_argument(
        "--realtime", "-r",
        action="store_true",
        help="Run real-time simulation with throttling"
    )
    mode_group.add_argument(
        "--interactive", "-i",
        action="store_true",
        help="Interactive mode with live feature output"
    )
    
    # Common configuration arguments
    parser.add_argument(
        "--sample-rate", "-s",
        type=float,
        default=1000.0,
        help="Sample rate in Hz (default: 1000.0)"
    )
    parser.add_argument(
        "--window-size", "-w",
        type=float,
        default=1.0,
        help="Rolling window size in seconds (default: 1.0)"
    )
    parser.add_argument(
        "--duration", "-d",
        type=float,
        default=10.0,
        help="Duration in seconds for benchmark/realtime modes (default: 10.0)"
    )
    
    # CSV-specific arguments
    parser.add_argument(
        "--path", "-p",
        type=str,
        default="data/sample_dataset_small.csv",
        help="Path to CSV file (default: data/sample_dataset_small.csv)"
    )
    parser.add_argument(
        "--value-column",
        type=str,
        default="strain",
        help="Column name for sensor values in CSV (default: strain)"
    )
    parser.add_argument(
        "--max-samples", "-n",
        type=int,
        default=None,
        help="Maximum number of samples to process (default: all)"
    )
    
    # CSV-specific arguments (removed - CSV mode not available)
    # parser.add_argument("--path", "-p", type=str, default="data/sample_dataset_small.csv")
    # parser.add_argument("--value-column", type=str, default="strain")
    # parser.add_argument("--max-samples", "-n", type=int, default=None)
    
    # Anomaly detection thresholds
    parser.add_argument(
        "--rms-threshold",
        type=float,
        default=3.0,
        help="RMS anomaly threshold multiple (default: 3.0)"
    )
    parser.add_argument(
        "--freq-threshold",
        type=float,
        default=5.0,
        help="Frequency shift threshold in Hz (default: 5.0)"
    )
    parser.add_argument(
        "--std-threshold",
        type=float,
        default=2.0,
        help="Std deviation threshold multiple (default: 2.0)"
    )
    
    # Output options
    parser.add_argument(
        "--json", "-j",
        action="store_true",
        help="Output results in JSON format"
    )
    parser.add_argument(
        "--quiet", "-q",
        action="store_true",
        help="Minimal output (only summary)"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Verbose output with detailed feature information"
    )
    
    # Sensor configuration for synthetic data
    parser.add_argument(
        "--fault-start",
        type=float,
        default=5.0,
        help="Time in seconds when fault starts (default: 5.0)"
    )
    parser.add_argument(
        "--fault-duration",
        type=float,
        default=1.0,
        help="Duration of fault in seconds (default: 1.0)"
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducibility (default: 42)"
    )
    
    return parser.parse_args()


def create_sensor_config(args) -> SensorConfig:
    """Create sensor configuration from arguments."""
    import numpy as np
    return SensorConfig(
        sample_rate=args.sample_rate,
        omega_n=2 * np.pi * 10.0,
        zeta=0.1,
        noise_std=0.1,
        fault_start=args.fault_start,
        fault_duration=args.fault_duration,
        seed=args.seed
    )


def format_features(features: WindowFeatures) -> str:
    """Format window features for display."""
    anomaly_marker = " [ANOMALY]" if features.is_anomaly else ""
    return (f"t={features.timestamp:.3f}s | "
            f"RMS={features.rms:.4f} | "
            f"Mean={features.mean:.4f} | "
            f"Std={features.std:.4f} | "
            f"DomFreq={features.dominant_freq:.2f}Hz | "
            f"Score={features.anomaly_score:.3f}{anomaly_marker}")


def run_benchmark_mode(args) -> dict:
    """Run throughput benchmark with synthetic data."""
    if not args.quiet:
        print(f"\n{'='*60}")
        print("RUNNING THROUGHPUT BENCHMARK")
        print(f"{'='*60}")
        print(f"Configuration:")
        print(f"  Sample rate: {args.sample_rate} Hz")
        print(f"  Window size: {args.window_size} seconds")
        print(f"  Duration: {args.duration} seconds")
        print(f"  Seed: {args.seed}")
        print(f"  Thresholds: RMS={args.rms_threshold}, Freq={args.freq_threshold}, Std={args.std_threshold}")
        print(f"\nStarting benchmark...")
    
    result = benchmark_throughput(
        sample_rate=args.sample_rate,
        duration=args.duration,
        window_size=args.window_size,
        rms_threshold_multiple=args.rms_threshold,
        freq_shift_threshold=args.freq_threshold,
        std_threshold_multiple=args.std_threshold
    )
    
    # Add configuration info to result
    result['config'] = {
        'sample_rate': args.sample_rate,
        'window_size': args.window_size,
        'duration': args.duration,
        'seed': args.seed
    }
    
    return result


def run_realtime_mode(args) -> dict:
    """Run real-time simulation with throttling."""
    if not args.quiet:
        print(f"\n{'='*60}")
        print("REAL-TIME SIMULATION")
        print(f"{'='*60}")
        print(f"Sample rate: {args.sample_rate} Hz")
        print(f"Window size: {args.window_size} seconds")
        print(f"Duration: {args.duration} seconds")
        print(f"Simulating real-time throttling...")
    
    config = create_sensor_config(args)
    sensor = VibrationSensor(config)
    
    fault_windows = [
        (config.fault_start, config.fault_start + config.fault_duration)
    ]
    
    processor = StreamingProcessor(
        sample_rate=args.sample_rate,
        window_size=args.window_size,
        fault_windows=fault_windows,
        simulate_real_time=True,
        rms_threshold_multiple=args.rms_threshold,
        freq_shift_threshold=args.freq_threshold,
        std_threshold_multiple=args.std_threshold
    )
    
    start_time = time.time()
    n_samples = int(args.duration * args.sample_rate)
    results = []
    
    for i in range(n_samples):
        sample = sensor.step()
        window_features = processor.process_sample(sample)
        if window_features is not None:
            results.append(window_features)
            if args.verbose:
                print(format_features(window_features))
    
    elapsed = time.time() - start_time
    throughput_stats = processor.get_throughput_stats()
    
    return {
        'mode': 'realtime',
        'n_samples': n_samples,
        'elapsed_time': elapsed,
        'actual_duration': elapsed,
        'throughput_stats': throughput_stats,
        'detection_stats': processor.detection_results,
        'n_windows': len(results),
        'config': {
            'sample_rate': args.sample_rate,
            'window_size': args.window_size,
            'duration': args.duration
        },
        'results': results if args.verbose else []
    }


def run_interactive_mode(args) -> dict:
    """Run interactive mode with live output."""
    print(f"\n{'='*60}")
    print("INTERACTIVE MODE (Press Ctrl+C to stop)")
    print(f"{'='*60}")
    print(f"Sample rate: {args.sample_rate} Hz")
    print(f"Window size: {args.window_size} seconds")
    print(f"\nPress Ctrl+C to stop at any time...")
    print(f"{'-'*60}")
    
    config = create_sensor_config(args)
    sensor = VibrationSensor(config)
    
    fault_windows = [
        (config.fault_start, config.fault_start + config.fault_duration)
    ]
    
    processor = StreamingProcessor(
        sample_rate=args.sample_rate,
        window_size=args.window_size,
        fault_windows=fault_windows,
        simulate_real_time=False,
        rms_threshold_multiple=args.rms_threshold,
        freq_shift_threshold=args.freq_threshold,
        std_threshold_multiple=args.std_threshold
    )
    
    results = []
    start_time = time.time()
    sample_count = 0
    
    try:
        while True:
            sample = sensor.step()
            window_features = processor.process_sample(sample)
            sample_count += 1
            
            if window_features is not None:
                results.append(window_features)
                # Print formatted output
                print(format_features(window_features))
                
                # Print summary every 10 windows
                if len(results) % 10 == 0:
                    elapsed = time.time() - start_time
                    stats = processor.get_throughput_stats()
                    print(f"  [Summary: {sample_count} samples, "
                          f"{len(results)} windows, "
                          f"{stats['samples_per_sec']:.1f} samples/sec]")
    except KeyboardInterrupt:
        print("\n\nStopped by user.")
    
    elapsed = time.time() - start_time
    throughput_stats = processor.get_throughput_stats()
    
    return {
        'mode': 'interactive',
        'n_samples': sample_count,
        'elapsed_time': elapsed,
        'throughput_stats': throughput_stats,
        'detection_stats': processor.detection_results,
        'n_windows': len(results),
        'config': {
            'sample_rate': args.sample_rate,
            'window_size': args.window_size
        }
    }


def print_benchmark_results(result: dict, json_output: bool = False) -> None:
    """Print benchmark results in a formatted way."""
    if json_output:
        print(json.dumps(result, indent=2, default=str))
        return
    
    print(f"\n{'='*60}")
    print("BENCHMARK RESULTS")
    print(f"{'='*60}")
    
    # Configuration
    config = result.get('config', {})
    print(f"\nConfiguration:")
    print(f"  Sample rate: {config.get('sample_rate', 'N/A')} Hz")
    print(f"  Window size: {config.get('window_size', 'N/A')} seconds")
    print(f"  Duration: {config.get('duration', 'N/A')} seconds")
    print(f"  Seed: {config.get('seed', 'N/A')}")
    
    # Throughput
    print(f"\nThroughput:")
    print(f"  Target samples: {result.get('n_samples', 0)}")
    print(f"  Actual duration: {result.get('actual_duration', 0):.4f} seconds")
    print(f"  Overall throughput: {result.get('samples_per_sec', 0):.2f} samples/sec")
    
    # Processing time stats
    stats = result.get('throughput_stats', {})
    print(f"\nProcessing Time:")
    print(f"  Avg: {stats.get('avg_processing_time_us', 0):.2f} us/sample")
    print(f"  P50: {stats.get('p50_processing_time_us', 0):.2f} us/sample")
    print(f"  P99: {stats.get('p99_processing_time_us', 0):.2f} us/sample")
    
    # Detection stats
    detection = result.get('detection_stats', {})
    print(f"\nDetection Statistics:")
    print(f"  True Positives: {detection.true_positives if hasattr(detection, 'true_positives') else detection.get('true_positives', 0)}")
    print(f"  False Positives: {detection.false_positives if hasattr(detection, 'false_positives') else detection.get('false_positives', 0)}")
    print(f"  False Negatives: {detection.false_negatives if hasattr(detection, 'false_negatives') else detection.get('false_negatives', 0)}")
    print(f"  True Negatives: {detection.true_negatives if hasattr(detection, 'true_negatives') else detection.get('true_negatives', 0)}")
    if hasattr(detection, 'precision'):
        print(f"  Precision: {detection.precision:.4f}")
        print(f"  Recall: {detection.recall:.4f}")
    else:
        precision = detection.get('precision', 0)
        recall = detection.get('recall', 0)
        print(f"  Precision: {precision:.4f}")
        print(f"  Recall: {recall:.4f}")
    
    print(f"\nWindow count: {result.get('n_windows', 0)}")
    
    # Pass/Fail
    if result.get('samples_per_sec', 0) >= 1000:
        print(f"\n[PASS] Achieved {result.get('samples_per_sec', 0):.0f}+ samples/sec")
    else:
        print(f"\n[FAIL] Only achieved {result.get('samples_per_sec', 0):.0f} samples/sec")
    
    print(f"{'='*60}\n")



def print_realtime_results(result: dict, json_output: bool = False) -> None:
    """Print real-time simulation results."""
    if json_output:
        print(json.dumps(result, indent=2, default=str))
        return
    
    print(f"\n{'='*60}")
    print("REAL-TIME SIMULATION RESULTS")
    print(f"{'='*60}")
    
    config = result.get('config', {})
    print(f"\nConfiguration:")
    print(f"  Sample rate: {config.get('sample_rate', 'N/A')} Hz")
    print(f"  Window size: {config.get('window_size', 'N/A')} seconds")
    print(f"  Duration: {config.get('duration', 'N/A')} seconds")
    
    print(f"\nSimulation:")
    print(f"  Target samples: {result.get('n_samples', 0)}")
    print(f"  Actual duration: {result.get('elapsed_time', 0):.4f} seconds")
    print(f"  Windows processed: {result.get('n_windows', 0)}")
    
    stats = result.get('throughput_stats', {})
    print(f"\nProcessing Time:")
    print(f"  Avg: {stats.get('avg_processing_time_us', 0):.2f} us/sample")
    print(f"  P50: {stats.get('p50_processing_time_us', 0):.2f} us/sample")
    print(f"  P99: {stats.get('p99_processing_time_us', 0):.2f} us/sample")
    
    detection = result.get('detection_stats', {})
    print(f"\nDetection Statistics:")
    print(f"  True Positives: {detection.true_positives if hasattr(detection, 'true_positives') else detection.get('true_positives', 0)}")
    print(f"  False Positives: {detection.false_positives if hasattr(detection, 'false_positives') else detection.get('false_positives', 0)}")
    print(f"  False Negatives: {detection.false_negatives if hasattr(detection, 'false_negatives') else detection.get('false_negatives', 0)}")
    print(f"  True Negatives: {detection.true_negatives if hasattr(detection, 'true_negatives') else detection.get('true_negatives', 0)}")
    if hasattr(detection, 'precision'):
        print(f"  Precision: {detection.precision:.4f}")
        print(f"  Recall: {detection.recall:.4f}")
    
    print(f"{'='*60}\n")


def print_interactive_results(result: dict, json_output: bool = False) -> None:
    """Print interactive mode results (minimal, as output was already shown)."""
    if json_output:
        print(json.dumps(result, indent=2, default=str))
        return
    
    print(f"\n{'='*60}")
    print("INTERACTIVE MODE SUMMARY")
    print(f"{'='*60}")
    
    config = result.get('config', {})
    print(f"\nConfiguration:")
    print(f"  Sample rate: {config.get('sample_rate', 'N/A')} Hz")
    print(f"  Window size: {config.get('window_size', 'N/A')} seconds")
    
    print(f"\nSession:")
    print(f"  Samples processed: {result.get('n_samples', 0)}")
    print(f"  Elapsed time: {result.get('elapsed_time', 0):.4f} seconds")
    print(f"  Windows generated: {result.get('n_windows', 0)}")
    
    stats = result.get('throughput_stats', {})
    print(f"\nThroughput:")
    print(f"  Avg processing time: {stats.get('avg_processing_time_us', 0):.2f} us/sample")
    
    detection = result.get('detection_stats', {})
    print(f"\nDetection Statistics:")
    print(f"  True Positives: {detection.true_positives if hasattr(detection, 'true_positives') else detection.get('true_positives', 0)}")
    print(f"  False Positives: {detection.false_positives if hasattr(detection, 'false_positives') else detection.get('false_positives', 0)}")
    if hasattr(detection, 'precision'):
        print(f"  Precision: {detection.precision:.4f}")
        print(f"  Recall: {detection.recall:.4f}")
    
    print(f"{'='*60}\n")


def main():
    """Main entry point."""
    args = parse_args()
    
    # Determine which mode to run
    if args.benchmark:
        result = run_benchmark_mode(args)
        print_benchmark_results(result, args.json)
    elif args.realtime:
        result = run_realtime_mode(args)
        print_realtime_results(result, args.json)
    elif args.interactive:
        result = run_interactive_mode(args)
        print_interactive_results(result, args.json)
    
    # Return exit code (0 for success, 1 if throughput target not met in benchmark)
    if args.benchmark:
        if result.get('samples_per_sec', 0) < 1000:
            sys.exit(1)
    
    sys.exit(0)


if __name__ == "__main__":
    main()
