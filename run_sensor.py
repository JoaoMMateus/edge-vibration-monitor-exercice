#!/usr/bin/env python
"""CLI script to run the vibration sensor."""

from edge.sensor import VibrationSensor, SensorConfig
import time

# Create sensor with configuration
config = SensorConfig(
    sample_rate=1000.0,
    omega_n=2 * 3.14159 * 10.0,  # 10 Hz
    zeta=0.1,
    noise_std=0.1,
    fault_start=4.0,  # Fault starts at 4 seconds
    fault_duration=1.0,
    seed=42
)

sensor = VibrationSensor(config)

# Generate and print first 5000 samples
print("Generating first 5000 samples from synthetic vibration sensor:")
print("-" * 60)
for i in range(5000):
    sample = sensor.step()
    print(f"Sample {i+1:3d}: t={sample.timestamp:.6f}s, "
          f"accel={sample.acceleration:.6f}, "
          f"vel={sample.velocity:.6f}, "
          f"disp={sample.displacement:.6f}")

print("-" * 60)
print("Done!")