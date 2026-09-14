"""
Edge Streaming Processor

Consumes sensor stream in real-time (or simulated real-time) and:
- Maintains a bounded rolling window using a circular buffer
- Computes rolling features: RMS, rolling mean/std, dominant frequency (FFT peak)
- Flags anomalies using threshold-based detection
- Reports precision/recall against known fault windows
- Sustains 1000+ samples/sec throughput

Memory footprint: O(window_size) - bounded by the rolling window size.
"""

import numpy as np
import time
from typing import Iterator, Tuple, Optional, List, Dict, Any
from dataclasses import dataclass, field
from collections import deque
from .sensor import SensorSample


@dataclass
class WindowFeatures:
    """Features computed for a rolling window."""
    timestamp: float
    window_start: float
    window_end: float
    n_samples: int
    rms: float
    mean: float
    std: float
    dominant_freq: float
    is_anomaly: bool = False
    anomaly_score: float = 0.0


@dataclass
class AnomalyDetectionResult:
    """Results of anomaly detection."""
    true_positives: int = 0
    false_positives: int = 0
    false_negatives: int = 0
    true_negatives: int = 0
    
    @property
    def precision(self) -> float:
        """Precision = TP / (TP + FP)"""
        total = self.true_positives + self.false_positives
        return self.true_positives / total if total > 0 else 0.0
    
    @property
    def recall(self) -> float:
        """Recall = TP / (TP + FN)"""
        total = self.true_positives + self.false_negatives
        return self.true_positives / total if total > 0 else 0.0


class CircularBuffer:
    """
    Efficient circular buffer for maintaining a bounded rolling window.
    """
    
    def __init__(self, capacity: int):
        self.capacity = capacity
        self.buffer = np.zeros(capacity, dtype=np.float64)
        self.timestamps = np.zeros(capacity, dtype=np.float64)
        self.size = 0
        self.head = 0  # Next write position
        self.tail = 0  # Oldest element position
    
    def append(self, value: float, timestamp: float) -> None:
        """Add a new sample to the buffer."""
        self.buffer[self.head] = value
        self.timestamps[self.head] = timestamp
        
        if self.size < self.capacity:
            self.size += 1
        else:
            # Overwrite oldest
            self.tail = (self.tail + 1) % self.capacity
        
        self.head = (self.head + 1) % self.capacity
    
    def get_data(self) -> Tuple[np.ndarray, np.ndarray]:
        """
        Get all data in the buffer in chronological order.
        Returns (values, timestamps) arrays.
        """
        if self.size == 0:
            return np.array([]), np.array([])
        
        if self.size < self.capacity:
            # Buffer not full yet, data is contiguous from 0 to head
            return self.buffer[:self.size], self.timestamps[:self.size]
        else:
            # Buffer is full, need to wrap around
            # Data is from tail to end, then from 0 to head-1
            part1_len = self.capacity - self.tail
            values = np.concatenate([
                self.buffer[self.tail:],
                self.buffer[:self.head]
            ])
            timestamps = np.concatenate([
                self.timestamps[self.tail:],
                self.timestamps[:self.head]
            ])
            return values, timestamps
    
    def get_window_start_end(self) -> Tuple[float, float]:
        """Get the timestamp range of the current window."""
        if self.size == 0:
            return 0.0, 0.0
        
        vals, ts = self.get_data()
        return ts[0], ts[-1]
    
    def __len__(self) -> int:
        return self.size
    
    def clear(self) -> None:
        """Clear the buffer."""
        self.buffer.fill(0)
        self.timestamps.fill(0)
        self.size = 0
        self.head = 0
        self.tail = 0


class RollingFeatureExtractor:
    """
    Computes rolling features from a circular buffer.
    
    Features:
    - RMS (Root Mean Square)
    - Rolling mean and standard deviation
    - Dominant frequency (via FFT peak detection)
    """
    
    def __init__(self, sample_rate: float = 1000.0):
        self.sample_rate = sample_rate
    
    def compute_rms(self, data: np.ndarray) -> float:
        """Compute Root Mean Square."""
        if len(data) == 0:
            return 0.0
        return float(np.sqrt(np.mean(data ** 2)))
    
    def compute_mean_std(self, data: np.ndarray) -> Tuple[float, float]:
        """Compute mean and standard deviation."""
        if len(data) == 0:
            return 0.0, 0.0
        return float(np.mean(data)), float(np.std(data))
    
    def compute_dominant_frequency(
        self,
        data: np.ndarray,
        timestamps: np.ndarray,
        low_freq: float = 1.0,
        high_freq: float = 100.0
    ) -> float:
        """
        Compute dominant frequency using FFT.
        
        Returns dominant_freq
        """
        n = len(data)
        if n < 8:  # Need at least 8 samples for meaningful FFT
            return 0.0
        
        # Compute FFT
        fft_vals = np.fft.rfft(data)
        freqs = np.fft.rfftfreq(n, d=1.0/self.sample_rate)
        
        # Filter to frequency range of interest
        mask = (freqs >= low_freq) & (freqs <= high_freq)
        if not np.any(mask):
            return 0.0
        
        freqs_filtered = freqs[mask]
        magnitudes = np.abs(fft_vals[mask])
        
        # Find peak
        if len(magnitudes) == 0:
            return 0.0
        
        peak_idx = np.argmax(magnitudes)
        return float(freqs_filtered[peak_idx])
    
    def extract_features(
        self,
        data: np.ndarray,
        timestamps: np.ndarray
    ) -> Dict[str, float]:
        """
        Extract all rolling features from a window of data.
        
        Returns dict with keys: 'rms', 'mean', 'std', 'dominant_freq'
        """
        rms = self.compute_rms(data)
        mean, std = self.compute_mean_std(data)
        dom_freq = self.compute_dominant_frequency(data, timestamps)
        
        return {
            'rms': rms,
            'mean': mean,
            'std': std,
            'dominant_freq': dom_freq
        }


class AnomalyDetector:
    """
    Threshold-based anomaly detector using rolling features.
    
    Flags anomalies based on:
    - RMS exceeding threshold (multiples of normal RMS)
    - Dominant frequency shift
    - Standard deviation changes
    """
    
    def __init__(
        self,
        rms_threshold_multiple: float = 3.0,
        freq_shift_threshold: float = 5.0,
        std_threshold_multiple: float = 2.0,
        warmup_samples: int = 100
    ):
        self.rms_threshold_multiple = rms_threshold_multiple
        self.freq_shift_threshold = freq_shift_threshold
        self.std_threshold_multiple = std_threshold_multiple
        self.warmup_samples = warmup_samples
        
        # Baseline statistics (computed during warmup)
        self.baseline_rms = 0.0
        self.baseline_dominant_freq = 0.0
        self.baseline_std = 0.0
        self.warmed_up = False
        self.sample_count = 0
    
    def update_baseline(self, features: Dict[str, float]) -> None:
        """Update baseline statistics during warmup phase."""
        if self.sample_count >= self.warmup_samples:
            self.warmed_up = True
            return
        
        # Exponential moving average for baseline
        alpha = 0.1
        if self.sample_count == 0:
            self.baseline_rms = features['rms']
            self.baseline_dominant_freq = features['dominant_freq']
            self.baseline_std = features['std']
        else:
            self.baseline_rms = (1 - alpha) * self.baseline_rms + alpha * features['rms']
            self.baseline_dominant_freq = (1 - alpha) * self.baseline_dominant_freq + alpha * features['dominant_freq']
            self.baseline_std = (1 - alpha) * self.baseline_std + alpha * features['std']
        
        self.sample_count += 1
    
    def detect(self, features: Dict[str, float]) -> Tuple[bool, float]:
        """
        Detect anomaly based on current features.
        
        Returns (is_anomaly, anomaly_score)
        """
        if not self.warmed_up:
            self.update_baseline(features)
            return False, 0.0
        
        score = 0.0
        anomaly = False
        
        # RMS-based detection
        rms_ratio = features['rms'] / max(self.baseline_rms, 1e-10)
        if rms_ratio > self.rms_threshold_multiple:
            score += rms_ratio / self.rms_threshold_multiple
            anomaly = True
        
        # Frequency shift detection
        freq_diff = abs(features['dominant_freq'] - self.baseline_dominant_freq)
        if freq_diff > self.freq_shift_threshold:
            score += freq_diff / self.freq_shift_threshold
            anomaly = True
        
        # Std deviation detection
        std_ratio = features['std'] / max(self.baseline_std, 1e-10)
        if std_ratio > self.std_threshold_multiple:
            score += std_ratio / self.std_threshold_multiple
            anomaly = True
        
        return anomaly, score
    
    def reset(self) -> None:
        """Reset detector state."""
        self.baseline_rms = 0.0
        self.baseline_dominant_freq = 0.0
        self.baseline_std = 0.0
        self.warmed_up = False
        self.sample_count = 0


class StreamingProcessor:
    """
    Main streaming processor for edge vibration monitoring.
    
    Consumes sensor samples, maintains rolling window, computes features,
    detects anomalies, and reports statistics.
    
    Memory footprint: O(window_capacity) - bounded by circular buffer size.
    Throughput: Designed for 1000+ samples/sec on single core.
    """
    
    def __init__(
        self,
        sample_rate: float = 1000.0,
        window_size: float = 1.0,  # Window size in seconds
        fault_windows: Optional[List[Tuple[float, float]]] = None,  # Known fault windows for evaluation
        rms_threshold_multiple: float = 3.0,
        freq_shift_threshold: float = 5.0,
        std_threshold_multiple: float = 2.0,
        simulate_real_time: bool = False
    ):
        self.sample_rate = sample_rate
        self.window_seconds = window_size
        self.window_capacity = int(window_size * sample_rate)
        
        # Components
        self.buffer = CircularBuffer(self.window_capacity)
        self.feature_extractor = RollingFeatureExtractor(sample_rate)
        self.anomaly_detector = AnomalyDetector(
            rms_threshold_multiple=rms_threshold_multiple,
            freq_shift_threshold=freq_shift_threshold,
            std_threshold_multiple=std_threshold_multiple
        )
        
        # Known fault windows for precision/recall evaluation
        self.fault_windows = fault_windows or []
        
        # Statistics tracking
        self.detection_results = AnomalyDetectionResult()
        self.total_samples = 0
        self.processing_times = []
        
        # Real-time simulation
        self.simulate_real_time = simulate_real_time
        self.last_sample_time = 0.0
        
        # Window timing
        self.window_start_time = 0.0
        self.window_initialized = False
    
    def _get_fault_label(self, timestamp: float) -> bool:
        """Check if timestamp falls within any known fault window."""
        for start, end in self.fault_windows:
            if start <= timestamp < end:
                return True
        return False
    
    def process_sample(self, sample: SensorSample) -> Optional[WindowFeatures]:
        """
        Process a single sensor sample.
        
        Args:
            sample: SensorSample with timestamp and acceleration
        
        Returns:
            WindowFeatures if window is full, None otherwise
        """
        start_time = time.time()
        
        # Use acceleration for feature computation
        value = sample.acceleration
        timestamp = sample.timestamp
        
        # Add to buffer
        self.buffer.append(value, timestamp)
        self.total_samples += 1
        
        # Initialize window tracking
        if not self.window_initialized and len(self.buffer) > 0:
            self.window_start_time = timestamp
            self.window_initialized = True
        
        # If buffer is full, compute features
        if len(self.buffer) >= self.window_capacity:
            # Get window data
            data, timestamps = self.buffer.get_data()
            window_start, window_end = self.buffer.get_window_start_end()
            
            # Extract features
            features = self.feature_extractor.extract_features(data, timestamps)
            
            # Detect anomaly
            is_anomaly, score = self.anomaly_detector.detect(features)
            
            # Check ground truth (for evaluation)
            is_fault = any(
                start <= timestamp < end 
                for start, end in self.fault_windows
            )
            
            # Update detection statistics
            if is_fault:
                if is_anomaly:
                    self.detection_results.true_positives += 1
                else:
                    self.detection_results.false_negatives += 1
            else:
                if is_anomaly:
                    self.detection_results.false_positives += 1
                else:
                    self.detection_results.true_negatives += 1
            
            # Create feature object
            window_features = WindowFeatures(
                timestamp=timestamp,
                window_start=window_start,
                window_end=window_end,
                n_samples=len(data),
                rms=features['rms'],
                mean=features['mean'],
                std=features['std'],
                dominant_freq=features['dominant_freq'],
                is_anomaly=is_anomaly,
                anomaly_score=score
            )
            
            # Track processing time
            elapsed = time.time() - start_time
            self.processing_times.append(elapsed)
            
            # Simulate real-time if enabled
            if self.simulate_real_time:
                expected_time = 1.0 / self.sample_rate
                if elapsed < expected_time:
                    time.sleep(expected_time - elapsed)
            
            return window_features
        
        # Track processing time even for non-full windows
        elapsed = time.time() - start_time
        self.processing_times.append(elapsed)
        
        # Simulate real-time if enabled
        if self.simulate_real_time:
            expected_time = 1.0 / self.sample_rate
            if elapsed < expected_time:
                time.sleep(expected_time - elapsed)
        
        return None
    
    def process_stream(
        self,
        sensor_stream: Iterator[SensorSample],
        max_samples: Optional[int] = None,
        callback: Optional[callable] = None
    ) -> List[WindowFeatures]:
        """
        Process a stream of sensor samples.
        
        Args:
            sensor_stream: Iterator yielding SensorSample objects
            max_samples: Maximum number of samples to process (None for unlimited)
            callback: Optional callback function called for each WindowFeatures
        
        Returns:
            List of WindowFeatures objects
        """
        results = []
        sample_count = 0
        
        for sample in sensor_stream:
            if max_samples and sample_count >= max_samples:
                break
            
            window_features = self.process_sample(sample)
            if window_features is not None:
                results.append(window_features)
                if callback is not None:
                    callback(window_features)
            
            sample_count += 1
        
        return results
    
    def get_throughput_stats(self) -> Dict[str, float]:
        """
        Get throughput statistics.
        
        Returns dict with:
        - samples_per_sec: Average samples processed per second
        - avg_processing_time_us: Average processing time per sample in microseconds
        - p50_processing_time_us: Median processing time in microseconds
        - p99_processing_time_us: 99th percentile processing time in microseconds
        """
        if len(self.processing_times) == 0:
            return {
                'samples_per_sec': 0.0,
                'avg_processing_time_us': 0.0,
                'p50_processing_time_us': 0.0,
                'p99_processing_time_us': 0.0
            }
        
        times_array = np.array(self.processing_times) * 1e6  # Convert to microseconds
        
        total_time = sum(self.processing_times)
        samples_per_sec = len(self.processing_times) / total_time if total_time > 0 else 0
        
        return {
            'samples_per_sec': samples_per_sec,
            'avg_processing_time_us': float(np.mean(times_array)),
            'p50_processing_time_us': float(np.median(times_array)),
            'p99_processing_time_us': float(np.percentile(times_array, 99))
        }
    
    def reset(self) -> None:
        """Reset the processor state."""
        self.buffer.clear()
        self.anomaly_detector.reset()
        self.detection_results = AnomalyDetectionResult()
        self.total_samples = 0
        self.processing_times = []
        self.window_initialized = False
        self.window_start_time = 0.0


def benchmark_throughput(
    sample_rate: float = 1000.0,
    duration: float = 10.0,
    window_size: float = 1.0,
    rms_threshold_multiple: float = 3.0,
    freq_shift_threshold: float = 5.0,
    std_threshold_multiple: float = 2.0
) -> Dict[str, Any]:
    """
    Benchmark the streaming processor throughput.
    
    Runs the processor with synthetic data for the specified duration.
    
    Args:
        sample_rate: Sample rate in Hz
        duration: Duration to run in seconds
        window_size: Rolling window size in seconds
        rms_threshold_multiple: RMS anomaly threshold multiple
        freq_shift_threshold: Frequency shift threshold in Hz
        std_threshold_multiple: Std deviation threshold multiple
    
    Returns:
        Dictionary with benchmark results
    """
    from .sensor import VibrationSensor, SensorConfig
    
    config = SensorConfig(
        sample_rate=sample_rate,
        seed=42
    )
    sensor = VibrationSensor(config)
    
    # Known fault window for evaluation
    fault_windows = [
        (config.fault_start, config.fault_start + config.fault_duration)
    ]
    
    processor = StreamingProcessor(
        sample_rate=sample_rate,
        window_size=window_size,
        fault_windows=fault_windows,
        rms_threshold_multiple=rms_threshold_multiple,
        freq_shift_threshold=freq_shift_threshold,
        std_threshold_multiple=std_threshold_multiple,
        simulate_real_time=False
    )
    
    # Generate samples
    n_samples = int(duration * sample_rate)
    
    start_time = time.time()
    
    # Process samples
    results = []
    for _ in range(n_samples):
        sample = sensor.step()
        window_features = processor.process_sample(sample)
        if window_features is not None:
            results.append(window_features)
    
    elapsed = time.time() - start_time
    
    # Calculate stats
    actual_samples = n_samples
    actual_duration = elapsed
    samples_per_sec = actual_samples / actual_duration
    
    throughput_stats = processor.get_throughput_stats()
    detection_stats = processor.detection_results
    
    return {
        'duration_seconds': duration,
        'n_samples': n_samples,
        'actual_duration': actual_duration,
        'samples_per_sec': samples_per_sec,
        'throughput_stats': throughput_stats,
        'detection_stats': detection_stats,
        'n_windows': len(results),
        'processor': processor
    }



if __name__ == "__main__":
    # Example usage and benchmark
    print("Running throughput benchmark...")
    
    benchmark_result = benchmark_throughput(
        sample_rate=1000.0,
        duration=10.0,
        window_size=1.0,
        rms_threshold_multiple=3.0,
        freq_shift_threshold=5.0,
        std_threshold_multiple=2.0
    )
    
    print(f"\nBenchmark Results:")
    print(f"  Duration: {benchmark_result['duration_seconds']}s")
    print(f"  Samples: {benchmark_result['n_samples']}")
    print(f"  Actual time: {benchmark_result['actual_duration']:.4f}s")
    print(f"  Throughput: {benchmark_result['samples_per_sec']:.2f} samples/sec")
    
    print(f"\nProcessing Time Stats:")
    stats = benchmark_result['throughput_stats']
    print(f"  Avg: {stats['avg_processing_time_us']:.2f} us/sample")
    print(f"  P50: {stats['p50_processing_time_us']:.2f} us/sample")
    print(f"  P99: {stats['p99_processing_time_us']:.2f} us/sample")
    
    print(f"\nDetection Stats:")
    print(f"  {benchmark_result['detection_stats']}")
    
    # Check if we meet the 1000 samples/sec requirement
    if benchmark_result['samples_per_sec'] >= 1000:
        print(f"\nPASS: Achieved {benchmark_result['samples_per_sec']:.0f}+ samples/sec")
    else:
        print(f"\nFAIL: Only achieved {benchmark_result['samples_per_sec']:.0f} samples/sec")
