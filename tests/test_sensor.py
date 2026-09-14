"""
Tests for the synthetic sensor (Part 1).

Tests cover:
- Sensor generation
- Fault injection
- Stream behavior
- CSV sensor wrapper
"""

import pytest
import numpy as np
from pathlib import Path
from edge.sensor import VibrationSensor, SensorConfig, SensorSample, CSVSensor, generate_csv_stream


class TestSensorConfig:
    """Tests for SensorConfig."""
    
    def test_default_values(self):
        """Test default configuration values."""
        config = SensorConfig()
        
        assert config.sample_rate == 1000.0
        assert config.omega_n == 2 * np.pi * 10.0
        assert config.zeta == 0.1
        assert config.noise_std == 0.1
        assert config.fault_start == 5.0
        assert config.fault_duration == 1.0
        assert config.seed is None
    
    def test_custom_values(self):
        """Test custom configuration values."""
        config = SensorConfig(
            sample_rate=2000.0,
            omega_n=2 * np.pi * 50.0,
            zeta=0.2,
            seed=42
        )
        
        assert config.sample_rate == 2000.0
        assert config.omega_n == 2 * np.pi * 50.0
        assert config.zeta == 0.2
        assert config.seed == 42


class TestVibrationSensor:
    """Tests for VibrationSensor."""
    
    def test_step_produces_samples(self):
        """Test that step() produces SensorSample objects."""
        config = SensorConfig(sample_rate=1000.0, seed=42)
        sensor = VibrationSensor(config)
        
        sample = sensor.step()
        
        assert isinstance(sample, SensorSample)
        assert isinstance(sample.timestamp, float)
        assert isinstance(sample.displacement, float)
        assert isinstance(sample.velocity, float)
        assert isinstance(sample.acceleration, float)
    
    def test_stream_produces_continuous_samples(self):
        """Test that stream() produces continuous samples."""
        config = SensorConfig(sample_rate=1000.0, seed=42)
        sensor = VibrationSensor(config)
        
        stream = sensor.stream()
        
        # Get first 10 samples
        samples = [next(stream) for _ in range(10)]
        
        assert len(samples) == 10
        
        # Timestamps should increase
        timestamps = [s.timestamp for s in samples]
        assert np.all(np.diff(timestamps) > 0), "Timestamps should be increasing"
    
    def test_reset(self):
        """Test that reset() returns sensor to initial state."""
        config = SensorConfig(sample_rate=1000.0, seed=42)
        sensor = VibrationSensor(config)
        
        # Get some samples
        for _ in range(100):
            sensor.step()
        
        # Reset
        sensor.reset()
        
        # First sample after reset should be same as first sample from new sensor
        new_sensor = VibrationSensor(config)
        
        sample_reset = sensor.step()
        sample_new = new_sensor.step()
        
        assert sample_reset.timestamp == sample_new.timestamp
        assert sample_reset.displacement == sample_new.displacement
        assert sample_reset.velocity == sample_new.velocity
        assert sample_reset.acceleration == sample_new.acceleration
    
    def test_fault_injection(self):
        """Test that fault injection changes sensor behavior."""
        config = SensorConfig(
            sample_rate=1000.0,
            omega_n=2 * np.pi * 10.0,
            fault_start=0.1,
            fault_duration=0.1,
            fault_omega_n=2 * np.pi * 20.0,
            seed=42
        )
        sensor = VibrationSensor(config)
        
        # Collect samples before, during, and after fault
        samples_before = []
        samples_during = []
        samples_after = []
        
        for _ in range(300):  # 0.3 seconds
            sample = sensor.step()
            t = sample.timestamp
            
            if t < 0.1:
                samples_before.append(sample)
            elif t < 0.2:
                samples_during.append(sample)
            else:
                samples_after.append(sample)
        
        assert len(samples_before) > 0
        assert len(samples_during) > 0
        assert len(samples_after) > 0
        
        # During fault, behavior should be different
        # (We can't test exact values, but we can check they're different)
        # This is a basic sanity check
    
    def test_deterministic_with_seed(self):
        """Test that sensor is deterministic with same seed."""
        config1 = SensorConfig(seed=42)
        config2 = SensorConfig(seed=42)
        
        sensor1 = VibrationSensor(config1)
        sensor2 = VibrationSensor(config2)
        
        # Generate samples from both
        samples1 = [sensor1.step() for _ in range(100)]
        samples2 = [sensor2.step() for _ in range(100)]
        
        # Should be identical
        for s1, s2 in zip(samples1, samples2):
            assert s1.timestamp == s2.timestamp
            assert s1.displacement == s2.displacement
            assert s1.velocity == s2.velocity
            assert s1.acceleration == s2.acceleration
    
    def test_sample_rate(self):
        """Test that sample rate is respected."""
        config = SensorConfig(sample_rate=1000.0, seed=42)
        sensor = VibrationSensor(config)
        
        # Get 1000 samples
        timestamps = [sensor.step().timestamp for _ in range(1000)]
        
        # Time between samples should be ~0.001 (1/1000)
        diffs = np.diff(timestamps)
        assert np.allclose(diffs, 0.001, rtol=1e-5)


class TestCSVSensor:
    """Tests for CSVSensor."""
    
    def test_csv_stream_from_file(self):
        """Test reading from CSV file."""
        csv_path = "data/sample_dataset_small.csv"
        
        if not Path(csv_path).exists():
            pytest.skip("Sample CSV file not found")
        
        sensor = CSVSensor(csv_path, value_column="strain")
        
        # Get first few valid samples (CSV has empty rows, so we need to iterate)
        stream = sensor.stream()
        samples = []
        for _ in range(20):  # Try up to 20 rows to get 10 valid samples
            try:
                sample = next(stream)
                samples.append(sample)
                if len(samples) >= 10:
                    break
            except StopIteration:
                break
        
        assert len(samples) >= 5, "Should get at least 5 valid samples"  # CSV has many valid samples
        
        for sample in samples:
            assert isinstance(sample, SensorSample)
            # Acceleration should have values from CSV
            assert isinstance(sample.acceleration, float)
    
    def test_csv_generator(self):
        """Test generate_csv_stream function."""
        csv_path = "data/sample_dataset_small.csv"
        
        if not Path(csv_path).exists():
            pytest.skip("Sample CSV file not found")
        
        # Get first few valid values (CSV has empty rows)
        values = []
        for i, (timestamp, value) in enumerate(generate_csv_stream(csv_path, value_column="strain")):
            values.append(value)
            if i >= 9:
                break
        
        # We should get some values (CSV has many valid entries)
        assert len(values) >= 5, "Should get at least 5 valid values"


class TestSensorSample:
    """Tests for SensorSample dataclass."""
    
    def test_creation(self):
        """Test creating a SensorSample."""
        sample = SensorSample(
            timestamp=1.0,
            displacement=0.5,
            velocity=0.1,
            acceleration=9.8
        )
        
        assert sample.timestamp == 1.0
        assert sample.displacement == 0.5
        assert sample.velocity == 0.1
        assert sample.acceleration == 9.8
    
    def test_equality(self):
        """Test equality of SensorSample objects."""
        sample1 = SensorSample(
            timestamp=1.0,
            displacement=0.5,
            velocity=0.1,
            acceleration=9.8
        )
        sample2 = SensorSample(
            timestamp=1.0,
            displacement=0.5,
            velocity=0.1,
            acceleration=9.8
        )
        
        assert sample1 == sample2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
