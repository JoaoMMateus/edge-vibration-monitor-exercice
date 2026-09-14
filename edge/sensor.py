"""
Part 1: Synthetic Vibration Sensor

A damped harmonic oscillator driven by noise, simulating vibration sensor data.
The system: x''(t) + 2*zeta*omega_n*x'(t) + omega_n^2*x(t) = F(t)

Where F(t) is mostly Gaussian noise with optional fault injection.
"""

import numpy as np
import time
from typing import Iterator, Tuple, Optional
from dataclasses import dataclass


@dataclass
class SensorConfig:
    """Configuration for the synthetic sensor."""
    sample_rate: float = 1000.0  # Hz
    omega_n: float = 2 * np.pi * 10.0  # Natural frequency (10 Hz)
    zeta: float = 0.1  # Damping ratio
    noise_std: float = 0.1  # Standard deviation of driving noise
    fault_start: float = 5.0  # Time to start fault injection (seconds)
    fault_duration: float = 1.0  # Duration of fault (seconds)
    fault_omega_n: float = 2 * np.pi * 15.0  # Natural frequency during fault
    fault_zeta: float = 0.05  # Damping ratio during fault
    seed: Optional[int] = None  # Random seed for reproducibility


@dataclass
class SensorSample:
    """A single sensor sample."""
    timestamp: float
    displacement: float
    velocity: float
    acceleration: float


class VibrationSensor:
    """
    Synthetic vibration sensor using a damped harmonic oscillator.
    
    Uses RK4 integration to solve the ODE:
    x'' + 2*zeta*omega_n*x' + omega_n^2*x = F(t)
    
    where F(t) ~ N(0, noise_std)
    """
    
    def __init__(self, config: SensorConfig):
        self.config = config
        self.rng = np.random.RandomState(config.seed)
        
        # State: [position, velocity]
        self.state = np.zeros(2)
        self.time = 0.0
        self.dt = 1.0 / config.sample_rate
        
        # Fault state
        self.fault_active = False
        self.fault_end_time = config.fault_start + config.fault_duration
    
    def _get_params(self, t: float) -> Tuple[float, float]:
        """Get current omega_n and zeta, accounting for fault injection."""
        if self.config.fault_start <= t < self.fault_end_time:
            return self.config.fault_omega_n, self.config.fault_zeta
        return self.config.omega_n, self.config.zeta
    
    def _rk4_step(self, t: float, state: np.ndarray) -> np.ndarray:
        """Perform one RK4 integration step."""
        omega_n, zeta = self._get_params(t)
        
        def f(t: float, y: np.ndarray) -> np.ndarray:
            x, v = y
            omega_n_curr, zeta_curr = self._get_params(t)
            # x'' = F(t) - 2*zeta*omega_n*x' - omega_n^2*x
            force = self.rng.normal(0, self.config.noise_std)
            a = force - 2 * zeta_curr * omega_n_curr * v - omega_n_curr**2 * x
            return np.array([v, a])
        
        k1 = f(t, state)
        k2 = f(t + self.dt/2, state + self.dt/2 * k1)
        k3 = f(t + self.dt/2, state + self.dt/2 * k2)
        k4 = f(t + self.dt, state + self.dt * k3)
        
        new_state = state + self.dt / 6 * (k1 + 2*k2 + 2*k3 + k4)
        return new_state
    
    def step(self) -> SensorSample:
        """Generate the next sample."""
        # Store current time before updating
        current_time = self.time
        
        # Perform integration step
        self.state = self._rk4_step(self.time, self.state)
        
        # Update time
        self.time += self.dt
        
        # Compute acceleration from the derivative
        # Using the current parameters
        omega_n, zeta = self._get_params(current_time)
        x, v = self.state
        a = self.rng.normal(0, self.config.noise_std) - \
            2 * zeta * omega_n * v - omega_n**2 * x
        
        return SensorSample(
            timestamp=current_time,
            displacement=float(x),
            velocity=float(v),
            acceleration=float(a)
        )
    
    def stream(self) -> Iterator[SensorSample]:
        """Generate samples as a stream (generator)."""
        while True:
            yield self.step()
    
    def reset(self):
        """Reset the sensor to initial state."""
        self.state = np.zeros(2)
        self.time = 0.0
        self.rng = np.random.RandomState(self.config.seed)
