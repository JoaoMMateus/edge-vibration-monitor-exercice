# Edge Vibration Monitor Exercise

An edge IoT pipeline for vibration monitoring with synthetic sensor data, streaming processing, anomaly detection, and cloud sync to S3 (with LocalStack mock).

> **Note**: This implementation was developed with extensive use of **Mistral Vibe**, a CLI coding agent.

## Overview

This project implements a complete edge vibration monitoring pipeline as specified in the take-home exercise:

- **Part 1**: Synthetic vibration sensor simulating a damped harmonic oscillator with configurable fault injection
- **Part 2**: Streaming processor with bounded rolling window, feature extraction (RMS, mean/std, FFT), and threshold-based anomaly detection
- **Part 3**: Cloud sync layer with batching, compression, LocalStack-mocked S3 uploads, durable file-based queue, retry with exponential backoff, and proper S3 object partitioning

## Requirements

- Python 3.9+
- pip

## Installation

### 1. Clone the repository

```bash
git clone <repository-url>
cd edge-vibration-monitor-exercice
```

### 2. Create and activate a virtual environment (recommended)

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

The core dependencies are:
- `numpy` - Standard library for numerical computing (justified: essential for signal processing, FFT, statistical computations; widely used, well-maintained, no security concerns)
- `pytest` - Testing framework

For optional LocalStack integration (see Part 3 below):
```bash
pip install boto3
```

## Project Structure

```
edge-vibration-monitor-exercice/
├── edge/
│   ├── __init__.py         # Package exports
│   ├── sensor.py           # Part 1: Synthetic vibration sensor (damped harmonic oscillator)
│   ├── processor.py        # Part 2: Streaming processor with circular buffer and anomaly detection
│   ├── sync.py             # Part 3: Cloud sync layer with LocalStack-mocked S3
│   └── trace.py            # Optional: Function call tracing for debugging
├── tests/
│   ├── __init__.py
│   ├── test_sensor.py      # Tests for sensor: generation, fault injection, stream behavior
│   ├── test_processor.py   # Tests for processor: bounded memory, features, anomaly detection, throughput
│   └── test_sync.py        # Tests for sync: durability, retry, recovery, S3 partitioning
├── data/
│   └── sample_dataset_small.csv  # Sample dataset (fallback option)
├── run_sensor.py           # CLI: Run sensor only (generates samples)
├── run_processor.py        # CLI: Run processor with benchmark/realtime/interactive modes
├── run_sync.py             # CLI: Run end-to-end pipeline (sensor -> processor -> sync)
├── requirements.txt
└── README.md
```

## Usage

### Running Tests

To run all tests (Parts 1-3):

```bash
pytest tests/ -v
```

To run tests for specific components:

```bash
# Test sensor only (Part 1)
pytest tests/test_sensor.py -v

# Test processor only (Part 2)
pytest tests/test_processor.py -v

# Test sync only (Part 3)
pytest tests/test_sync.py -v
```

### Running the Sensor (Part 1)

Generate samples from the synthetic vibration sensor:

```bash
python run_sensor.py
```

This outputs the first 5000 samples with timestamps and acceleration values. The sensor uses RK4 integration to solve the damped harmonic oscillator equation with configurable parameters.

### Running the Processor (Part 2)

#### Benchmark Mode (verify 1000+ samples/sec throughput)

```bash
python run_processor.py --benchmark
```

Options:
```bash
python run_processor.py --benchmark --sample-rate 1000 --window-size 1.0 --duration 10.0
python run_processor.py --benchmark --json  # Output as JSON
```

#### Real-time Simulation Mode

Simulate real-time processing with throttling to match the sample rate:

```bash
python run_processor.py --realtime --duration 15
```

#### Interactive Mode

Run with live feature output (press Ctrl+C to stop):

```bash
python run_processor.py --interactive --sample-rate 1000 --window-size 0.5
```

### Running the Cloud Sync (Part 3)

#### End-to-End Pipeline with Mock S3

Run the complete pipeline: sensor generates samples, processor computes features, sync batches and uploads to mocked S3:

```bash
python run_sync.py --duration 10 --batch-size 5 --failure-rate 0.3
```

This demonstrates:
- Streaming sensor data
- Rolling window processing with anomaly detection
- Batching of window features + 10Hz downsampled acceleration
- Compression and upload to mocked S3
- Simulated intermittent connectivity (30% failure rate)
- Automatic retry with exponential backoff
- Durable queue that survives across restarts

#### With LocalStack (Real S3 Mock)

First, start LocalStack:

```bash
# Using Docker (recommended)
docker run -d -p 4566:4566 -p 4510:4510 localstack/localstack

# Or using localstack CLI
localstack start -d
```

Then configure AWS credentials for LocalStack:

```bash
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1
```

Run the sync with LocalStack endpoint:

```bash
python run_sync.py --duration 5 --batch-size 3 --failure-rate 0.2 --endpoint-url http://localhost:4566
```

Verify uploaded objects:

```bash
# Using AWS CLI with LocalStack endpoint
aws --endpoint-url=http://localhost:4566 s3 ls s3://edge-vibration-data/ --recursive
```

### End-to-End: Generator -> Processor -> Sync

The complete data flow is:

```
VibrationSensor (1kHz stream)
    ↓ (SensorSample: timestamp, displacement, velocity, acceleration)
StreamingProcessor (rolling 1-2s window)
    ↓ (WindowFeatures: RMS, mean, std, dominant_freq, is_anomaly, anomaly_score)
    ↓ (10Hz downsampled acceleration samples)
CloudSync (batches every N windows or M seconds)
    ↓ (Batch: compressed JSON with windows + 10Hz samples)
    ↓ (FileQueue: durable storage on disk if upload fails)
    ↓ (MockS3Client / LocalStack: S3 upload with retry)
```

To run the entire pipeline end-to-end:

```bash
# Full pipeline with LocalStack
python run_sync.py --duration 30 --batch-size 10 --failure-rate 0.1

# Or programmatically:
from edge.sensor import VibrationSensor, SensorConfig
from edge.processor import StreamingProcessor
from edge.sync import CloudSync, S3Config

# 1. Create sensor
config = SensorConfig(sample_rate=1000, fault_start=5.0, fault_duration=1.0)
sensor = VibrationSensor(config)

# 2. Create processor
processor = StreamingProcessor(
    sample_rate=1000,
    window_size=1.0,
    fault_windows=[(5.0, 6.0)]
)

# 3. Create sync with LocalStack
s3_config = S3Config(
    bucket_name="edge-vibration-data",
    endpoint_url="http://localhost:4566",
    aws_access_key_id="test",
    aws_secret_access_key="test",
    use_ssl=False
)
sync = CloudSync(
    sensor_id="sensor_01",
    device_id="edge_01",
    s3_config=s3_config,
    batch_size=10,
    failure_rate=0.1,
    queue_dir="unsent_batches"
)

# 4. Process stream
for sample in sensor.stream():
    window_features = processor.process_sample(sample)
    if window_features:
        sync.add_window(window_features)

sync.flush()
sync.shutdown()
```

## Features

### Part 1: Synthetic Vibration Sensor

- **Physics Model**: Damped harmonic oscillator `x''(t) + 2*zeta*omega_n*x'(t) + omega_n^2*x(t) = F(t)`
- **Numerical Integration**: RK4 (Runge-Kutta 4th order) for accurate time-domain simulation
- **Driving Force**: Gaussian noise `F(t) ~ N(0, noise_std)` representing normal operating vibration
- **Fault Injection**: Configurable time window where `omega_n` and `zeta` shift to simulate bearing wear or mounting issues
- **Stream Interface**: Generator/iterator pattern (`stream()` method) emits samples one at a time
- **Output**: Each sample contains timestamp, displacement, velocity, and acceleration

### Part 2: Streaming Processor

- **Bounded Rolling Window**: Circular buffer with O(capacity) memory footprint, where capacity = window_size * sample_rate
- **Rolling Features**:
  - **RMS**: Root Mean Square for energy content
  - **Mean/Std**: Statistical measures of signal amplitude
  - **Dominant Frequency**: FFT peak detection in configurable frequency range (default 1-100Hz)
- **Anomaly Detection**: Threshold-based detector using:
  - RMS exceeding multiple of baseline (default 3x)
  - Dominant frequency shift from baseline (default >5Hz)
  - Standard deviation exceeding multiple of baseline (default 2x)
- **Baseline Estimation**: Exponential moving average during warmup phase (default 100 samples)
- **Precision/Recall**: Tracked against known fault windows for evaluation
- **Throughput**: Designed for 1000+ samples/sec on single core; benchmark proves capability

### Part 3: Cloud Sync Layer

- **Batching**: Configurable batch size (number of windows) or max batch age before forced upload
- **Compression**: Gzip compression of batch payloads before upload
- **10Hz Downsampling**: Includes 10Hz acceleration samples alongside window features in batches
- **S3 Partitioning**: Objects stored as `device_id/sensor_id/year=YYYY/month=MM/day=DD/hour=HH/batch_id.json.gz`
- **Durable Queue**: File-based queue (`FileQueue`) stores unsent batches as compressed JSON files on disk
- **Retry Logic**: Exponential backoff retry (delay * 2^retry_count) with configurable max retries
- **Background Processing**: Non-blocking upload thread; processor continues during uploads
- **Recovery**: `recover()` method reloads unsent batches from disk after restart
- **Failure Simulation**: Configurable failure rate for testing connectivity issues
- **LocalStack Support**: Can connect to LocalStack for realistic S3 mocking

## Configuration

### Sensor Configuration (SensorConfig)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `sample_rate` | 1000.0 | Sampling rate in Hz |
| `omega_n` | 2*pi*10 | Natural frequency in rad/s (10Hz default) |
| `zeta` | 0.1 | Damping ratio |
| `noise_std` | 0.1 | Standard deviation of Gaussian noise driving force |
| `fault_start` | 5.0 | Time (seconds) to start fault injection |
| `fault_duration` | 1.0 | Duration of fault (seconds) |
| `fault_omega_n` | 2*pi*15 | Natural frequency during fault (15Hz) |
| `fault_zeta` | 0.05 | Damping ratio during fault |
| `seed` | None | Random seed for reproducibility |

### Processor Configuration (StreamingProcessor)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `sample_rate` | 1000.0 | Sampling rate in Hz |
| `window_size` | 1.0 | Rolling window size in seconds |
| `fault_windows` | [] | Known fault windows for precision/recall evaluation |
| `rms_threshold_multiple` | 3.0 | RMS anomaly threshold (multiples of baseline) |
| `freq_shift_threshold` | 5.0 | Frequency shift threshold in Hz |
| `std_threshold_multiple` | 2.0 | Std deviation threshold (multiples of baseline) |
| `simulate_real_time` | False | Enable real-time simulation throttling |
| `warmup_samples` | 100 | Number of samples for baseline estimation |

### Sync Configuration (CloudSync)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `sensor_id` | "vibration_01" | Sensor identifier |
| `device_id` | "edge_device_01" | Device identifier |
| `batch_size` | 100 | Number of windows per batch |
| `max_batch_age` | 30.0 | Max age in seconds before forcing upload |
| `queue_dir` | "unsent_batches" | Directory for durable queue storage |
| `retry_delay` | 1.0 | Initial retry delay in seconds |
| `max_retries` | 5 | Maximum number of retry attempts |
| `failure_rate` | 0.0 | Simulated network failure rate (0.0 to 1.0) |
| `background` | True | Run uploads in background thread |

### S3 Configuration (S3Config)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `bucket_name` | "edge-vibration-data" | S3 bucket name |
| `region` | "us-east-1" | AWS region |
| `endpoint_url` | None | Custom endpoint (use "http://localhost:4566" for LocalStack) |
| `aws_access_key_id` | None | AWS access key (use "test" for LocalStack) |
| `aws_secret_access_key` | None | AWS secret key (use "test" for LocalStack) |
| `use_ssl` | True | Use SSL (set False for LocalStack) |

## Troubleshooting

### Common Issues

**Issue: Throughput below 1000 samples/sec**
- Check CPU usage during benchmark
- Reduce window size (try 0.5s instead of 1.0s)
- Verify numpy is installed (not pure Python fallback)
- Check for other CPU-intensive processes

**Issue: Sync uploads failing**
- Verify LocalStack is running: `docker ps` should show the LocalStack container
- Check credentials: `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` must be set
- Verify endpoint: `http://localhost:4566` for LocalStack
- Check failure rate setting (try 0.0 to disable failures)

**Issue: Memory growing unbounded**
- Verify circular buffer is working: `len(processor.buffer)` should never exceed `window_capacity`
- Check for memory leaks in custom code
- Use memory profiler to identify growing objects

**Issue: File queue not persisting**
- Verify queue directory exists and is writable
- Check that batches are being written to disk
- Verify same queue directory is used across restarts

### Debug Mode

Enable verbose tracing:

```bash
export EDGE_TRACE_LOG_FILE=debug.log
python run_sync.py --duration 5
# Check debug.log for detailed function call tracing
```

## License

This project is provided as an exercise solution. Feel free to modify and adapt as needed.
