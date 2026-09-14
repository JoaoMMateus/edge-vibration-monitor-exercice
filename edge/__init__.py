"""
Edge vibration monitoring package.

Contains:
- sensor.py: Synthetic vibration sensor
- processor.py: Streaming processor
"""

from .sensor import VibrationSensor, SensorConfig, SensorSample, CSVSensor
from .processor import (
    StreamingProcessor,
    CircularBuffer,
    WindowFeatures,
    AnomalyDetectionResult,
    RollingFeatureExtractor,
    AnomalyDetector,
    benchmark_throughput,
    run_from_csv
)

__version__ = "0.1.0"
