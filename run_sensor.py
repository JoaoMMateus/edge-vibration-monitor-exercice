#!/usr/bin/env python
"""CLI script to run the vibration sensor with simple trace logging."""

from edge.sensor import VibrationSensor, SensorConfig
from edge.trace import log_call, log_output

config = SensorConfig(
    sample_rate=1000.0,
    omega_n=2 * 3.14159 * 10.0,
    zeta=0.1,
    noise_std=0.1,
    fault_start=4.0,
    fault_duration=1.0,
    seed=42,
)

sensor = VibrationSensor(config)
log_call("VibrationSensor.__init__")
log_output("VibrationSensor.__init__", sensor)

print("Generating first 5000 samples from synthetic vibration sensor:")
print("-" * 60)
for i in range(5000):
    log_call("VibrationSensor.step")
    sample = sensor.step()
    log_output("VibrationSensor.step", sample)
    print(f"Sample {i+1:3d}: t={sample.timestamp:.6f}s, "
          f"accel={sample.acceleration:.6f}, "
          f"vel={sample.velocity:.6f}, "
          f"disp={sample.displacement:.6f}")

print("-" * 60)
print("Done!")