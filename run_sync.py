#!/usr/bin/env python
"""CLI script that runs the cloud sync layer using processor output."""

import argparse
import shutil
import tempfile

import numpy as np

from edge.sensor import SensorConfig, VibrationSensor
from edge.processor import StreamingProcessor
from edge.sync import CloudSync
from edge.trace import log_call, log_output


def parse_args():
    """Parse command-line options for the sync demo."""
    parser = argparse.ArgumentParser(
        description="Run the mock cloud sync layer with WindowFeatures created by StreamingProcessor"
    )
    parser.add_argument("--sample-rate", type=float, default=1000.0)
    parser.add_argument("--window-size", type=float, default=1.0)
    parser.add_argument("--duration", type=float, default=1.0)
    parser.add_argument("--batch-size", type=int, default=5)
    parser.add_argument("--max-batch-age", type=float, default=60.0)
    parser.add_argument("--queue-dir", type=str, default=None)
    parser.add_argument("--failure-rate", type=float, default=0.0)
    parser.add_argument("--rms-threshold", type=float, default=3.0)
    parser.add_argument("--freq-threshold", type=float, default=5.0)
    parser.add_argument("--std-threshold", type=float, default=2.0)
    parser.add_argument("--fault-start", type=float, default=5.0)
    parser.add_argument("--fault-duration", type=float, default=1.0)
    parser.add_argument("--seed", type=int, default=42)
    return parser.parse_args()


def build_demo_pipeline(args):
    """Create the sensor, processor, and sync objects for the demo."""
    log_call("build_demo_pipeline")
    queue_dir = args.queue_dir or tempfile.mkdtemp(prefix="edge_sync_queue_")

    config = SensorConfig(
        sample_rate=args.sample_rate,
        omega_n=2 * np.pi * 10.0,
        zeta=0.1,
        noise_std=0.1,
        fault_start=args.fault_start,
        fault_duration=args.fault_duration,
        seed=args.seed,
    )

    sensor = VibrationSensor(config)
    processor = StreamingProcessor(
        sample_rate=args.sample_rate,
        window_size=args.window_size,
        fault_windows=[(args.fault_start, args.fault_start + args.fault_duration)],
        rms_threshold_multiple=args.rms_threshold,
        freq_shift_threshold=args.freq_threshold,
        std_threshold_multiple=args.std_threshold,
        simulate_real_time=False,
    )

    sync = CloudSync(
        sensor_id="demo_sensor",
        device_id="demo_device",
        s3_config=None,
        batch_size=args.batch_size,
        max_batch_age=args.max_batch_age,
        queue_dir=queue_dir,
        retry_delay=0.05,
        max_retries=2,
        failure_rate=args.failure_rate,
        background=False,
    )

    result = (sensor, processor, sync, queue_dir)
    log_output("build_demo_pipeline", result)
    return result


def main() -> None:
    """Run the pipeline end-to-end and push processor output into the sync queue."""
    log_call("main")
    args = parse_args()
    sensor, processor, sync, queue_dir = build_demo_pipeline(args)

    try:
        samples = int(args.duration * args.sample_rate)
        print("Starting processor -> sync demo...")
        print(f"Sampling {samples} sensor samples at {args.sample_rate:.1f} Hz")

        count = 0
        for _ in range(samples):
            log_call("VibrationSensor.step")
            sample = sensor.step()
            log_output("VibrationSensor.step", sample)

            log_call("StreamingProcessor.process_sample")
            window_features = processor.process_sample(sample)
            log_output("StreamingProcessor.process_sample", window_features)

            if window_features is not None:
                log_call("CloudSync.add_window")
                sync.add_window(window_features)
                log_output("CloudSync.add_window", window_features)
                count += 1

        log_call("CloudSync.flush")
        sync.flush()
        log_output("CloudSync.flush", sync.get_stats())

        print("Processor windows emitted:", count)
        print("Uploaded batches:", sync.uploaded_batches)
        print("Failed batches:", sync.failed_batches)
        print("Mock S3 objects:", len(sync.s3_client.uploaded_objects))
        print("Stats:", sync.get_stats())
    finally:
        if sync is not None:
            log_call("CloudSync.shutdown")
            sync.shutdown()
            log_output("CloudSync.shutdown", True)
        if queue_dir and queue_dir.startswith(tempfile.gettempdir()):
            shutil.rmtree(queue_dir, ignore_errors=True)

    log_output("main", {"uploaded_batches": sync.uploaded_batches})


if __name__ == "__main__":
    main()
