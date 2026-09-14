"""Minimal cloud sync layer for edge batches and an in-memory mock S3 client."""

import gzip
import json
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from queue import Queue
from threading import Lock, Thread
from typing import Any, Dict, List, Optional, Sequence

try:
    import boto3
except Exception:  # pragma: no cover - boto3 is optional for environments without AWS.
    boto3 = None


@dataclass
class Batch:
    """A batch of window features and optional downsampled 10Hz samples."""
    batch_id: str
    sensor_id: str
    device_id: str
    windows: List[Dict[str, Any]]
    created_at: float
    compression: str = "gzip"
    samples: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'batch_id': self.batch_id,
            'sensor_id': self.sensor_id,
            'device_id': self.device_id,
            'windows': self.windows,
            'created_at': self.created_at,
            'compression': self.compression,
            'samples': self.samples,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), default=str)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Batch':
        payload = dict(data)
        payload.setdefault('samples', [])
        return cls(**payload)


class S3Config:
    """Configuration for S3 upload."""
    
    def __init__(
        self,
        bucket_name: str = "edge-vibration-data",
        region: str = "us-east-1",
        endpoint_url: Optional[str] = None,  # For LocalStack: http://localhost:4566
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        use_ssl: bool = True
    ):
        self.bucket_name = bucket_name
        self.region = region
        self.endpoint_url = endpoint_url
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.use_ssl = use_ssl


class Downsampler:
    """Take a high-rate stream and keep one sample every source/target-rate stride.

    Example: a 1000Hz stream and a target of 10Hz produces one sample per
    100 source samples, giving a clean 10Hz downsampled payload.
    """

    def __init__(self, source_sample_rate: float = 1000.0, target_sample_rate: float = 10.0):
        self.source_sample_rate = float(source_sample_rate)
        self.target_sample_rate = float(target_sample_rate)
        if self.source_sample_rate <= 0:
            raise ValueError("source_sample_rate must be positive")
        if self.target_sample_rate <= 0:
            raise ValueError("target_sample_rate must be positive")
        self.step = max(1, int(round(self.source_sample_rate / self.target_sample_rate)))

    def process(self, samples: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Return every Nth record from a source sample sequence."""
        if not samples:
            return []
        return [dict(sample) for idx, sample in enumerate(samples) if idx % self.step == 0]


class RealS3Client:
    """A boto3-backed S3 client aimed at LocalStack or a real S3-compatible endpoint."""

    def __init__(self, config: Optional[S3Config] = None, use_localstack: bool = True):
        if boto3 is None:
            raise RuntimeError("boto3 is required for RealS3Client")
        self.config = config or S3Config()
        self.use_localstack = use_localstack

        self._client = boto3.client(
            's3',
            region_name=self.config.region,
            endpoint_url=self.config.endpoint_url,
            aws_access_key_id=self.config.aws_access_key_id or 'test',
            aws_secret_access_key=self.config.aws_secret_access_key or 'test',
            use_ssl=self.config.use_ssl,
        )
        self._resource = boto3.resource(
            's3',
            region_name=self.config.region,
            endpoint_url=self.config.endpoint_url,
            aws_access_key_id=self.config.aws_access_key_id or 'test',
            aws_secret_access_key=self.config.aws_secret_access_key or 'test',
            use_ssl=self.config.use_ssl,
        )

    def upload(self, key: str, data: bytes) -> bool:
        """Upload a gzip-compressed payload object to the configured bucket."""
        try:
            self._client.put_object(Bucket=self.config.bucket_name, Key=key, Body=data)
            return True
        except Exception:
            return False

    def upload_batch(self, batch: Batch) -> bool:
        """Upload a batch object using the same deterministic key partitioning as the mock client."""
        timestamp = datetime.fromtimestamp(batch.created_at)
        key = self._get_object_key(batch, timestamp)
        data = batch.to_json().encode("utf-8")
        compressed = gzip.compress(data)
        return self.upload(key, compressed)

    def _get_object_key(self, batch: Batch, timestamp: datetime) -> str:
        return (
            f"{batch.device_id}/{batch.sensor_id}/"
            f"year={timestamp.year}/"
            f"month={timestamp.month:02d}/"
            f"day={timestamp.day:02d}/"
            f"hour={timestamp.hour:02d}/"
            f"{batch.batch_id}.json.gz"
        )

    def get_object(self, key: str) -> Optional[bytes]:
        """Read back an object from S3 using boto3 resource semantics."""
        try:
            return self._resource.Object(self.config.bucket_name, key).get()['Body'].read()
        except Exception:
            return None

    def list_objects(self, prefix: str = "") -> List[str]:
        """List object keys filtered by a prefix inside the configured bucket."""
        try:
            bucket = self._resource.Bucket(self.config.bucket_name)
            return [obj.key for obj in bucket.objects.filter(Prefix=prefix)]
        except Exception:
            return []


class FileQueue:
    """
    Durable file-based queue for storing unsent batches.
    
    Batches are stored as gzipped JSON files in a directory.
    Survives process restarts.
    """
    
    def __init__(self, queue_dir: str = "unsent_batches"):
        self.queue_dir = Path(queue_dir)
        self.queue_dir.mkdir(parents=True, exist_ok=True)
        self._lock = Lock()
    
    def enqueue(self, batch: Batch) -> Path:
        """Add a batch to the queue."""
        with self._lock:
            # Create a unique filename
            filename = self.queue_dir / f"{batch.batch_id}.json.gz"
            
            # Serialize and compress
            data = batch.to_json().encode('utf-8')
            compressed = gzip.compress(data)
            
            # Write to file
            with open(filename, 'wb') as f:
                f.write(compressed)
            
            return filename
    
    def dequeue(self) -> Optional[Batch]:
        """Remove and return the oldest batch from the queue."""
        with self._lock:
            # Find all batch files
            batch_files = sorted(self.queue_dir.glob("*.json.gz"), key=lambda p: p.stat().st_mtime)
            
            if not batch_files:
                return None
            
            # Read the oldest file
            oldest = batch_files[0]
            with open(oldest, 'rb') as f:
                compressed = f.read()
            
            # Decompress and deserialize
            data = gzip.decompress(compressed).decode('utf-8')
            batch = Batch.from_dict(json.loads(data))
            
            # Remove the file
            oldest.unlink()
            
            return batch
    
    def peek(self) -> Optional[Batch]:
        """Return the oldest batch without removing it."""
        with self._lock:
            batch_files = sorted(self.queue_dir.glob("*.json.gz"), key=lambda p: p.stat().st_mtime)
            
            if not batch_files:
                return None
            
            with open(batch_files[0], 'rb') as f:
                compressed = f.read()
            
            data = gzip.decompress(compressed).decode('utf-8')
            return Batch.from_dict(json.loads(data))
    
    def size(self) -> int:
        """Return the number of batches in the queue."""
        with self._lock:
            return len(list(self.queue_dir.glob("*.json.gz")))
    
    def clear(self) -> None:
        """Clear all batches from the queue."""
        with self._lock:
            for f in self.queue_dir.glob("*.json.gz"):
                f.unlink()
    
    def list_all(self) -> List[Batch]:
        """List all batches in the queue."""
        with self._lock:
            batches = []
            for f in sorted(self.queue_dir.glob("*.json.gz"), key=lambda p: p.stat().st_mtime):
                with open(f, 'rb') as fh:
                    compressed = fh.read()
                data = gzip.decompress(compressed).decode('utf-8')
                batches.append(Batch.from_dict(json.loads(data)))
            return batches


class MockS3Client:
    """In-memory mock S3 client for offline and testable upload behavior."""

    def __init__(
        self,
        config: Optional[S3Config] = None,
        failure_rate: float = 0.0,
        use_localstack: bool = False,
    ):
        self.config = config or S3Config()
        self.failure_rate = failure_rate
        self.use_localstack = use_localstack
        self.uploaded_objects: Dict[str, bytes] = {}
        self._failure_counter = 0

    def should_fail(self) -> bool:
        """Return True when the current simulated request should fail."""
        if self.failure_rate <= 0:
            return False
        if self.failure_rate >= 1.0:
            return True
        return (self._failure_counter % 100) < int(self.failure_rate * 100)

    def upload(self, key: str, data: bytes) -> bool:
        """Store a mock object and return whether the upload succeeded."""
        self._failure_counter += 1
        if self.should_fail():
            return False
        self.uploaded_objects[key] = data
        return True

    def upload_batch(self, batch: Batch) -> bool:
        """Compress and upload a Batch object using the mock storage."""
        timestamp = datetime.fromtimestamp(batch.created_at)
        key = self._get_object_key(batch, timestamp)
        data = batch.to_json().encode("utf-8")
        compressed = gzip.compress(data)
        return self.upload(key, compressed)

    def _get_object_key(self, batch: Batch, timestamp: datetime) -> str:
        """Create a deterministic object key layout for the mock bucket."""
        return (
            f"{batch.device_id}/{batch.sensor_id}/"
            f"year={timestamp.year}/"
            f"month={timestamp.month:02d}/"
            f"day={timestamp.day:02d}/"
            f"hour={timestamp.hour:02d}/"
            f"{batch.batch_id}.json.gz"
        )

    def get_object(self, key: str) -> Optional[bytes]:
        """Return an uploaded object from the in-memory mock store."""
        return self.uploaded_objects.get(key)

    def list_objects(self, prefix: str = "") -> List[str]:
        """Return all mock keys beginning with a prefix."""
        return [key for key in self.uploaded_objects if key.startswith(prefix)]

    def set_failure_rate(self, rate: float) -> None:
        """Set the failure rate for the simulated network path."""
        self.failure_rate = rate


class CloudSync:
    """
    Cloud sync layer for uploading processed window features.

    Features:
    - Batches windows into configurable batch sizes
    - Compresses batches before upload
    - Uses durable file-based queue for reliability
    - Retries failed uploads with exponential backoff
    - Non-blocking (runs in background thread)
    - Decoupled from processor
    """

    def __init__(
        self,
        sensor_id: str = "vibration_01",
        device_id: str = "edge_device_01",
        s3_config: Optional[S3Config] = None,
        batch_size: int = 100,  # Number of windows per batch
        max_batch_age: float = 30.0,  # Max age in seconds before forcing upload
        queue_dir: str = "unsent_batches",
        retry_delay: float = 1.0,  # Initial retry delay in seconds
        max_retries: int = 5,
        failure_rate: float = 0.0,  # Simulated failure rate
        background: bool = True,  # Run uploads in background thread
        s3_client: Optional[Any] = None,
        use_localstack: bool = False,
    ):
        self.sensor_id = sensor_id
        self.device_id = device_id
        self.batch_size = batch_size
        self.max_batch_age = max_batch_age
        self.queue_dir = queue_dir
        self.retry_delay = retry_delay
        self.max_retries = max_retries
        self.failure_rate = failure_rate
        self.background = background

        if s3_client is not None:
            self.s3_client = s3_client
        elif use_localstack:
            cfg = s3_config or S3Config(endpoint_url='http://localhost:4566', use_ssl=False)
            self.s3_client = RealS3Client(config=cfg, use_localstack=True)
        else:
            self.s3_client = MockS3Client(
                config=s3_config,
                failure_rate=failure_rate,
                use_localstack=False,
            )
        self.file_queue = FileQueue(queue_dir)

        # Current batch
        self.current_batch: Optional[Batch] = None
        self.current_batch_windows: List[Dict[str, Any]] = []
        self.current_batch_samples: List[Dict[str, Any]] = []
        self.current_batch_created: float = 0.0
        
        # Threading
        self._queue_lock = Lock()
        self._upload_thread: Optional[Thread] = None
        self._shutdown = False
        self._upload_queue = Queue()
        
        # Statistics
        self.uploaded_batches = 0
        self.failed_batches = 0
        self.retry_counts = []
        
        # Start background thread
        if self.background:
            self._start_upload_thread()
    
    def _start_upload_thread(self) -> None:
        """Start the background upload thread."""
        self._upload_thread = Thread(target=self._upload_worker, daemon=True)
        self._upload_thread.start()
    
    def _upload_worker(self) -> None:
        """Background worker that processes upload queue."""
        while not self._shutdown:
            try:
                # Try to upload with timeout
                item = self._upload_queue.get(timeout=1.0)
                if item == "SHUTDOWN":
                    break
                
                batch, retry_count = item
                self._upload_batch_with_retry(batch, retry_count)
            except Exception:
                # Queue is empty, continue
                continue
    
    def add_window(self, window_features: Any) -> None:
        """Add a processor window object or dictionary payload to the current batch.

        This sync layer accepts either a raw mapping produced by the sample
        script or a real `WindowFeatures` dataclass object from the stream
        processor, then normalizes it to the same upload record structure.
        """
        with self._queue_lock:
            now = time.time()

            if self.current_batch is None:
                self.current_batch = Batch(
                    batch_id=str(uuid.uuid4()),
                    sensor_id=self.sensor_id,
                    device_id=self.device_id,
                    windows=[],
                    created_at=now,
                )
                self.current_batch_windows = []
                self.current_batch_samples = []
                self.current_batch_created = now

            if hasattr(window_features, "__dict__"):
                extracted = {
                    key: value
                    for key, value in window_features.__dict__.items()
                    if not key.startswith("_")
                }
            elif isinstance(window_features, dict):
                extracted = {
                    key: value
                    for key, value in window_features.items()
                    if not callable(value)
                }
            else:
                raise TypeError("window_features must be a dict or WindowFeatures object")

            self.current_batch_windows.append(extracted)

            should_upload = False
            if len(self.current_batch_windows) >= self.batch_size:
                should_upload = True
            if (now - self.current_batch_created) >= self.max_batch_age:
                should_upload = True

            if should_upload:
                self.current_batch.windows = self.current_batch_windows
                self.current_batch.created_at = self.current_batch_created
                self.current_batch.samples = self.current_batch_samples.copy()

                batch_to_upload = Batch(
                    batch_id=self.current_batch.batch_id,
                    sensor_id=self.current_batch.sensor_id,
                    device_id=self.current_batch.device_id,
                    windows=self.current_batch_windows.copy(),
                    created_at=self.current_batch_created,
                    samples=self.current_batch_samples.copy(),
                )

                self.file_queue.enqueue(batch_to_upload)

                if self.background:
                    self._upload_queue.put((batch_to_upload, 0))
                else:
                    self._upload_batch_with_retry(batch_to_upload, 0)

                self.current_batch = None
                self.current_batch_windows = []
                self.current_batch_samples = []
    
    def add_samples(self, samples: List[Dict[str, Any]]) -> None:
        """Attach the 10Hz sample payload to the current batch for optional LocalStack upload."""
        with self._queue_lock:
            if self.current_batch is None:
                self.current_batch = Batch(
                    batch_id=str(uuid.uuid4()),
                    sensor_id=self.sensor_id,
                    device_id=self.device_id,
                    windows=[],
                    created_at=time.time(),
                )
                self.current_batch_windows = []
                self.current_batch_samples = []
                self.current_batch_created = self.current_batch.created_at
            self.current_batch_samples.extend(samples)

    def _upload_batch_with_retry(self, batch: Batch, retry_count: int = 0) -> bool:
        """
        Upload a batch with exponential backoff retry.

        Returns True if successful, False if max retries exceeded.
        """
        success = self.s3_client.upload_batch(batch)
        
        if success:
            # Remove from file queue
            self.file_queue.dequeue()  # Remove the oldest (this batch)
            self.uploaded_batches += 1
            self.retry_counts.append(retry_count)
            return True
        
        # Retry with exponential backoff
        if retry_count >= self.max_retries:
            self.failed_batches += 1
            return False
        
        # Calculate delay with exponential backoff
        delay = self.retry_delay * (2 ** retry_count)
        time.sleep(delay)
        
        # Queue for retry
        if self.background:
            self._upload_queue.put((batch, retry_count + 1))
        else:
            return self._upload_batch_with_retry(batch, retry_count + 1)
        
        return False
    
    def flush(self) -> None:
        """Flush any pending batches."""
        with self._queue_lock:
            if self.current_batch is not None and len(self.current_batch_windows) > 0:
                self.current_batch.windows = self.current_batch_windows
                self.current_batch.created_at = self.current_batch_created
                self.current_batch.samples = self.current_batch_samples.copy()

                batch_to_upload = Batch(
                    batch_id=self.current_batch.batch_id,
                    sensor_id=self.current_batch.sensor_id,
                    device_id=self.current_batch.device_id,
                    windows=self.current_batch_windows.copy(),
                    created_at=self.current_batch_created,
                    samples=self.current_batch_samples.copy(),
                )

                self.file_queue.enqueue(batch_to_upload)

                if self.background:
                    self._upload_queue.put((batch_to_upload, 0))
                else:
                    self._upload_batch_with_retry(batch_to_upload, 0)

                self.current_batch = None
                self.current_batch_windows = []
                self.current_batch_samples = []
        
        # No join() here; queue is file-backed and the sync thread owns retry
        # timing independently of the batch enqueue path.
    
    def shutdown(self) -> None:
        """Shutdown the sync layer."""
        self._shutdown = True
        self.flush()
        
        # Signal upload thread to stop
        if self.background and self._upload_thread:
            self._upload_queue.put("SHUTDOWN")
            self._upload_thread.join(timeout=5.0)
    
    def recover(self) -> None:
        """
        Recover unsent batches from disk after restart.
        
        Called during initialization or after a restart.
        """
        # Load all unsent batches from file queue
        unsent_batches = self.file_queue.list_all()
        
        for batch in unsent_batches:
            # Queue each for upload
            if self.background:
                self._upload_queue.put((batch, 0))
            else:
                self._upload_batch_with_retry(batch, 0)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get sync statistics without extra scientific-library dependencies."""
        return {
            'uploaded_batches': self.uploaded_batches,
            'failed_batches': self.failed_batches,
            'pending_batches': self.file_queue.size(),
            'avg_retries': sum(self.retry_counts) / len(self.retry_counts) if self.retry_counts else 0,
            'max_retries': max(self.retry_counts) if self.retry_counts else 0,
        }
    
    def set_failure_rate(self, rate: float) -> None:
        """Set simulated failure rate."""
        self.failure_rate = rate
        if hasattr(self.s3_client, 'set_failure_rate'):
            self.s3_client.set_failure_rate(rate)
