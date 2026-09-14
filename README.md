# Edge Vibration Monitor Exercise

An edge IoT pipeline for vibration monitoring with synthetic sensor data, streaming processing, and anomaly detection.

## Overview

This project implements a two-part system:

- **Part 1**: Synthetic vibration sensor simulating a damped harmonic oscillator with fault injection
- **Part 2**: Streaming processor that computes rolling features, detects anomalies, and reports precision/recall statistics

> **Note**: This implementation was developed with assistance from Mistral Vibe, a CLI coding agent.

## Requirements

- Python 3.9+
- pip

## Installation

### 1. Clone the repository

```bash
cd /path/to/your/directory
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

If no requirements.txt exists, install the required packages directly:

```bash
pip install numpy pytest
```

## Project Structure

```
edge-vibration-monitor-exercice/
├── edge/
│   ├── __init__.py
│   ├── sensor.py      # Synthetic vibration sensor (Part 1)
│   └── processor.py   # Streaming processor (Part 2)
├── tests/
│   ├── __init__.py
│   ├── test_sensor.py
│   └── test_processor.py
├── run_processor.py   # CLI for running the processor
└── README.md
```

## Usage

### Running Tests

To run all tests:

```bash
pytest tests/ -v
```

To run tests for specific components:

```bash
# Test sensor only
pytest tests/test_sensor.py -v

# Test processor only
pytest tests/test_processor.py -v
```

### Running the Benchmark

Use the CLI script to run the throughput benchmark:

```bash
python run_processor.py --benchmark
```

Options:

```bash
python run_processor.py --benchmark --sample-rate 1000 --window-size 1.0 --duration 10.0
```

Arguments:
- `--sample-rate` / `-s`: Sample rate in Hz (default: 1000.0)
- `--window-size` / `-w`: Rolling window size in seconds (default: 1.0)
- `--duration` / `-d`: Duration in seconds (default: 10.0)
- `--rms-threshold`: RMS anomaly threshold multiple (default: 3.0)
- `--freq-threshold`: Frequency shift threshold in Hz (default: 5.0)
- `--std-threshold`: Standard deviation threshold multiple (default: 2.0)
- `--seed`: Random seed for reproducibility (default: 42)
- `--json`: Output results as JSON
- `--quiet` / `-q`: Suppress progress output

### Running Real-time Simulation

Simulate real-time processing with throttling:

```bash
python run_processor.py --realtime --duration 15
```

### Running Interactive Mode

Run with live feature output:

```bash
python run_processor.py --interactive
```

## Features

### Synthetic Sensor (Part 1)
- Damped harmonic oscillator model: `x''(t) + 2*zeta*omega_n*x'(t) + omega_n^2*x(t) = F(t)`
- RK4 numerical integration
- Gaussian noise as driving force F(t)
- Configurable fault injection (shifts omega_n and/or zeta)
- Stream output via generator/iterator

### Streaming Processor (Part 2)
- Bounded rolling window (circular buffer)
- Rolling features: RMS, mean, std, dominant frequency (FFT peak)
- Threshold-based anomaly detection
- Precision/recall statistics against known fault windows
- Throughput: 1000+ samples/sec on single core

## Configuration

### Sensor Configuration (SensorConfig)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `sample_rate` | 1000.0 | Sampling rate in Hz |
| `omega_n` | 2π×10 | Natural frequency (rad/s) |
| `zeta` | 0.1 | Damping ratio |
| `noise_std` | 0.1 | Standard deviation of Gaussian noise |
| `fault_start` | 5.0 | Time to start fault injection (seconds) |
| `fault_duration` | 1.0 | Duration of fault (seconds) |
| `fault_omega_n` | 2π×15 | Natural frequency during fault |
| `fault_zeta` | 0.05 | Damping ratio during fault |
| `seed` | None | Random seed for reproducibility |

### Processor Configuration (StreamingProcessor)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `sample_rate` | 1000.0 | Sampling rate in Hz |
| `window_size` | 1.0 | Rolling window size in seconds |
| `fault_windows` | [] | Known fault windows for evaluation |
| `rms_threshold_multiple` | 3.0 | RMS anomaly threshold (multiples of baseline) |
| `freq_shift_threshold` | 5.0 | Frequency shift threshold in Hz |
| `std_threshold_multiple` | 2.0 | Std deviation threshold (multiples of baseline) |
| `simulate_real_time` | False | Enable real-time simulation throttling |

## Performance

The system is designed to sustain at least 1000 samples/second on a single core on typical laptop hardware. Run the benchmark to verify:

```bash
python run_processor.py --benchmark
```

## Development

### Adding New Tests

Add test files in the `tests/` directory following the existing pattern. Run with:

```bash
pytest tests/test_your_module.py -v
```

### Code Style

This project uses standard Python conventions. No additional linting configuration is required.

## License

This project is provided as an exercise solution. Feel free to modify and adapt as needed.
