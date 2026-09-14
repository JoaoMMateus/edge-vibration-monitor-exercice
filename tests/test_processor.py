"""
Tests for the streaming processor (Part 2).

Tests cover:
- Ring buffer bounded memory behavior
- Feature extraction (RMS, mean, std, FFT)
- Anomaly detection
- Throughput performance
"""

import pytest
import numpy as np
import time
import tempfile
import shutil
from edge.processor import (
    CircularBuffer,
    RollingFeatureExtractor,
    AnomalyDetector,
    StreamingProcessor,
    WindowFeatures,
    AnomalyDetectionResult,
    benchmark_throughput
)
from edge.sensor import VibrationSensor, SensorConfig, SensorSample


class TestCircularBuffer:
    """Tests for CircularBuffer."""
    
    def test_bounded_size(self):
        """Test that buffer size is bounded."""
        capacity = 100
        buffer = CircularBuffer(capacity)
        
        # Add more than capacity
        for i in range(200):
            buffer.append(float(i), float(i))
        
        # Size should be exactly capacity
        assert len(buffer) == capacity
    
    def test_fifo_behavior(self):
        """Test that buffer follows FIFO (oldest is overwritten)."""
        capacity = 5
        buffer = CircularBuffer(capacity)
        
        # Fill buffer
        for i in range(5):
            buffer.append(float(i), float(i))
        
        # Add more (should overwrite oldest)
        for i in range(5, 10):
            buffer.append(float(i), float(i))
        
        # Get data
        values, timestamps = buffer.get_data()
        
        # Should have last 5 elements
        assert len(values) == 5
        np.testing.assert_array_equal(values, [5.0, 6.0, 7.0, 8.0, 9.0])
    
    def test_chronological_order(self):
        """Test that data is returned in chronological order."""
        capacity = 10
        buffer = CircularBuffer(capacity)
        
        # Add out of order timestamps (but append in order)
        for i in range(20):
            buffer.append(float(i), float(i * 10))
        
        values, timestamps = buffer.get_data()
        
        # Timestamps should be in ascending order
        assert np.all(np.diff(timestamps) >= 0), "Timestamps should be sorted"
    
    def test_empty_buffer(self):
        """Test empty buffer behavior."""
        buffer = CircularBuffer(10)
        
        values, timestamps = buffer.get_data()
        assert len(values) == 0
        assert len(timestamps) == 0
        assert len(buffer) == 0
    
    def test_clear(self):
        """Test clearing the buffer."""
        buffer = CircularBuffer(10)
        
        for i in range(5):
            buffer.append(float(i), float(i))
        
        assert len(buffer) == 5
        
        buffer.clear()
        
        assert len(buffer) == 0
        values, timestamps = buffer.get_data()
        assert len(values) == 0


class TestRollingFeatureExtractor:
    """Tests for RollingFeatureExtractor."""
    
    def test_rms_computation(self):
        """Test RMS computation."""
        extractor = RollingFeatureExtractor(sample_rate=1000.0)
        
        # Simple test signal
        data = np.array([0.0, 1.0, 0.0, -1.0])
        timestamps = np.array([0.0, 0.001, 0.002, 0.003])
        
        rms = extractor.compute_rms(data)
        
        # RMS of [0, 1, 0, -1] = sqrt((0+1+0+1)/4) = sqrt(0.5)
        expected = np.sqrt(0.5)
        assert abs(rms - expected) < 1e-10
    
    def test_mean_std_computation(self):
        """Test mean and std computation."""
        extractor = RollingFeatureExtractor(sample_rate=1000.0)
        
        data = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
        timestamps = np.linspace(0, 0.005, 5)
        
        mean, std = extractor.compute_mean_std(data)
        
        expected_mean = 3.0
        expected_std = np.std(data)
        
        assert abs(mean - expected_mean) < 1e-10
        assert abs(std - expected_std) < 1e-10
    
    def test_dominant_frequency(self):
        """Test dominant frequency detection."""
        extractor = RollingFeatureExtractor(sample_rate=1000.0)
        
        # Create a 10Hz sine wave
        t = np.linspace(0, 1, 1000)  # 1 second at 1000 Hz
        data = np.sin(2 * np.pi * 10 * t)
        timestamps = t
        
        dom_freq = extractor.compute_dominant_frequency(data, timestamps, low_freq=5.0, high_freq=15.0)
        
        # Should detect ~10 Hz
        assert abs(dom_freq - 10.0) < 0.5, f"Expected ~10Hz, got {dom_freq}Hz"
    
    def test_empty_input(self):
        """Test handling of empty input."""
        extractor = RollingFeatureExtractor(sample_rate=1000.0)
        
        rms = extractor.compute_rms(np.array([]))
        assert rms == 0.0
        
        mean, std = extractor.compute_mean_std(np.array([]))
        assert mean == 0.0
        assert std == 0.0
        
        dom_freq = extractor.compute_dominant_frequency(np.array([]), np.array([]))
        assert dom_freq == 0.0
    
    def test_extract_all_features(self):
        """Test extracting all features at once."""
        extractor = RollingFeatureExtractor(sample_rate=1000.0)
        
        data = np.random.randn(1000)
        timestamps = np.linspace(0, 1, 1000)
        
        features = extractor.extract_features(data, timestamps)
        
        assert 'rms' in features
        assert 'mean' in features
        assert 'std' in features
        assert 'dominant_freq' in features
        
        # All should be numeric
        assert isinstance(features['rms'], float)
        assert isinstance(features['mean'], float)
        assert isinstance(features['std'], float)
        assert isinstance(features['dominant_freq'], float)


class TestAnomalyDetector:
    """Tests for AnomalyDetector."""
    
    def test_warmup_phase(self):
        """Test that detector is in warmup initially."""
        detector = AnomalyDetector(warmup_samples=10)
        
        assert not detector.warmed_up
        assert detector.sample_count == 0
    
    def test_baseline_update(self):
        """Test baseline update during warmup."""
        detector = AnomalyDetector(warmup_samples=5)
        
        features = {'rms': 1.0, 'mean': 0.0, 'std': 0.5, 'dominant_freq': 10.0}
        
        # First update
        detector.update_baseline(features)
        assert detector.sample_count == 1
        assert detector.baseline_rms == 1.0
        
        # Second update (EMA)
        features2 = {'rms': 2.0, 'mean': 0.0, 'std': 0.5, 'dominant_freq': 10.0}
        detector.update_baseline(features2)
        assert detector.sample_count == 2
        # Should be EMA of 1.0 and 2.0
        assert detector.baseline_rms > 1.0 and detector.baseline_rms < 2.0
    
    def test_anomaly_detection_rms(self):
        """Test anomaly detection based on RMS."""
        detector = AnomalyDetector(
            rms_threshold_multiple=2.0,
            warmup_samples=1
        )
        
        # Warmup with normal data
        features = {'rms': 1.0, 'mean': 0.0, 'std': 0.5, 'dominant_freq': 10.0}
        detector.update_baseline(features)
        detector.update_baseline(features)
        
        # Should be warmed up now
        assert detector.warmed_up
        
        # Normal data (should not flag)
        is_anomaly, score = detector.detect(features)
        assert not is_anomaly
        
        # High RMS (should flag)
        features_high = {**features, 'rms': 5.0}  # 5x baseline
        is_anomaly, score = detector.detect(features_high)
        assert is_anomaly
        assert score > 0
    
    def test_anomaly_detection_freq_shift(self):
        """Test anomaly detection based on frequency shift."""
        detector = AnomalyDetector(
            freq_shift_threshold=5.0,
            warmup_samples=1
        )
        
        # Warmup
        features = {'rms': 1.0, 'mean': 0.0, 'std': 0.5, 'dominant_freq': 10.0}
        detector.update_baseline(features)
        detector.update_baseline(features)
        
        # Frequency shift (should flag)
        features_shifted = {**features, 'dominant_freq': 20.0}  # 10 Hz shift
        is_anomaly, score = detector.detect(features_shifted)
        assert is_anomaly
    
    def test_reset(self):
        """Test resetting the detector."""
        detector = AnomalyDetector(warmup_samples=5)
        
        features = {'rms': 1.0, 'mean': 0.0, 'std': 0.5, 'dominant_freq': 10.0}
        detector.update_baseline(features)
        
        detector.reset()
        
        assert not detector.warmed_up
        assert detector.sample_count == 0
        assert detector.baseline_rms == 0.0


class TestStreamingProcessor:
    """Tests for StreamingProcessor."""
    
    def test_bounded_memory(self):
        """Test that processor memory is bounded."""
        processor = StreamingProcessor(
            sample_rate=1000.0,
            window_size=1.0  # 1000 samples
        )
        
        # Process many samples
        for i in range(5000):
            sample = SensorSample(
                timestamp=float(i) / 1000.0,
                displacement=0.0,
                velocity=0.0,
                acceleration=float(np.random.randn())
            )
            processor.process_sample(sample)
        
        # Buffer should never exceed window capacity
        assert len(processor.buffer) <= processor.window_capacity
    
    def test_window_features(self):
        """Test that window features are computed correctly."""
        window_size = 0.1  # 100 ms = 100 samples at 1000 Hz
        processor = StreamingProcessor(
            sample_rate=1000.0,
            window_size=window_size
        )
        
        # Process exactly one window
        for i in range(100):
            sample = SensorSample(
                timestamp=float(i) / 1000.0,
                displacement=0.0,
                velocity=0.0,
                acceleration=1.0  # Constant value
            )
            result = processor.process_sample(sample)
        
        # Last call should return features
        assert result is not None
        assert result.n_samples == 100
        assert abs(result.mean - 1.0) < 1e-10
        assert abs(result.std - 0.0) < 1e-10
    
    def test_anomaly_flagging(self):
        """Test that anomalies are flagged."""
        # Use synthetic sensor with known fault
        config = SensorConfig(
            sample_rate=1000.0,
            omega_n=2 * np.pi * 10.0,
            zeta=0.1,
            noise_std=0.1,
            fault_start=0.5,  # Start fault at 0.5 seconds
            fault_duration=0.2,
            fault_omega_n=2 * np.pi * 20.0,  # Change frequency
            fault_zeta=0.05,
            seed=42
        )
        sensor = VibrationSensor(config)
        
        fault_windows = [(0.5, 0.7)]
        
        processor = StreamingProcessor(
            sample_rate=1000.0,
            window_size=0.2,  # 200 ms window
            fault_windows=fault_windows,
            rms_threshold_multiple=1.5,  # Lower threshold for test
            freq_shift_threshold=5.0
        )
        
        # Process samples through fault window
        n_samples = 1000  # 1 second
        for _ in range(n_samples):
            sample = sensor.step()
            processor.process_sample(sample)
        
        # Check detection results
        result = processor.detection_results
        
        # Should have some detections
        total_detections = result.true_positives + result.false_positives
        assert total_detections > 0, "Should detect some anomalies"
    
    def test_throughput(self):
        """Test that throughput meets requirements."""
        result = benchmark_throughput(
            sample_rate=1000.0,
            duration=1.0,  # 1 second of data
            window_size=1.0
        )
        
        # Should process at least 1000 samples/sec
        assert result['samples_per_sec'] >= 1000, \
            f"Throughput {result['samples_per_sec']:.2f} samples/sec < 1000"
        
        # Processing time should be reasonable
        stats = result['throughput_stats']
        assert stats['avg_processing_time_us'] < 1000, \
            f"Avg processing time {stats['avg_processing_time_us']:.2f} us > 1000 us"


class TestBenchmark:
    """Tests for benchmark functions."""
    
    def test_benchmark_returns_results(self):
        """Test that benchmark returns expected results."""
        result = benchmark_throughput(
            sample_rate=1000.0,
            duration=0.5,
            window_size=0.5
        )
        
        assert 'duration_seconds' in result
        assert 'n_samples' in result
        assert 'actual_duration' in result
        assert 'samples_per_sec' in result
        assert 'throughput_stats' in result
        assert 'detection_stats' in result
        assert 'n_windows' in result
    
    def test_benchmark_detection_stats(self):
        """Test that detection stats are computed."""
        result = benchmark_throughput(
            sample_rate=1000.0,
            duration=2.0,
            window_size=0.5
        )
        
        stats = result['detection_stats']
        assert isinstance(stats, AnomalyDetectionResult)
        
        # Should have some results
        assert stats.true_positives >= 0
        assert stats.false_positives >= 0


class TestAnomalyDetectionResult:
    """Tests for AnomalyDetectionResult."""
    
    def test_precision_calculation(self):
        """Test precision calculation."""
        result = AnomalyDetectionResult(
            true_positives=10,
            false_positives=2,
            false_negatives=1,
            true_negatives=100
        )
        
        # Precision = TP / (TP + FP) = 10 / 12
        expected = 10 / 12
        assert abs(result.precision - expected) < 1e-10
    
    def test_recall_calculation(self):
        """Test recall calculation."""
        result = AnomalyDetectionResult(
            true_positives=10,
            false_positives=2,
            false_negatives=3,
            true_negatives=100
        )
        
        # Recall = TP / (TP + FN) = 10 / 13
        expected = 10 / 13
        assert abs(result.recall - expected) < 1e-10
    
    def test_zero_division_handling(self):
        """Test handling of zero division cases."""
        # No positive or negative predictions
        result = AnomalyDetectionResult()
        assert result.precision == 0.0
        assert result.recall == 0.0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
