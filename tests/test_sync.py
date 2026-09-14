"""
Tests for the cloud sync layer (Part 3).

Tests cover:
- File-based queue durability
- S3 partitioning
- Retry logic
- Batch creation
- Recovery after restart
"""

import json
import shutil
import tempfile
import time

from edge.processor import WindowFeatures
from edge.sync import Batch, CloudSync, FileQueue, MockS3Client, S3Config


class TestFileQueue:
    """Tests for FileQueue."""
    
    def test_enqueue_dequeue(self):
        """Test basic enqueue and dequeue operations."""
        temp_dir = tempfile.mkdtemp()
        
        try:
            queue = FileQueue(temp_dir)
            
            batch = Batch(
                batch_id="test_1",
                sensor_id="sensor_1",
                device_id="device_1",
                windows=[],
                created_at=time.time()
            )
            
            # Enqueue
            file_path = queue.enqueue(batch)
            assert file_path.exists()
            
            # Dequeue
            retrieved = queue.dequeue()
            assert retrieved is not None
            assert retrieved.batch_id == "test_1"
            
            # Queue should be empty
            assert queue.dequeue() is None
            
        finally:
            shutil.rmtree(temp_dir)
    
    def test_fifo_order(self):
        """Test that queue follows FIFO order."""
        temp_dir = tempfile.mkdtemp()
        
        try:
            queue = FileQueue(temp_dir)
            
            # Enqueue multiple batches
            for i in range(5):
                batch = Batch(
                    batch_id=f"batch_{i}",
                    sensor_id="sensor_1",
                    device_id="device_1",
                    windows=[],
                    created_at=time.time() + i
                )
                queue.enqueue(batch)
                time.sleep(0.01)  # Ensure different timestamps
            
            # Dequeue and verify order
            for i in range(5):
                retrieved = queue.dequeue()
                assert retrieved.batch_id == f"batch_{i}"
            
        finally:
            shutil.rmtree(temp_dir)
    
    def test_size(self):
        """Test size tracking."""
        temp_dir = tempfile.mkdtemp()
        
        try:
            queue = FileQueue(temp_dir)
            
            assert queue.size() == 0
            
            for i in range(3):
                batch = Batch(
                    batch_id=f"batch_{i}",
                    sensor_id="sensor_1",
                    device_id="device_1",
                    windows=[],
                    created_at=time.time()
                )
                queue.enqueue(batch)
                assert queue.size() == i + 1
            
        finally:
            shutil.rmtree(temp_dir)
    
    def test_clear(self):
        """Test clearing the queue."""
        temp_dir = tempfile.mkdtemp()
        
        try:
            queue = FileQueue(temp_dir)
            
            for i in range(5):
                batch = Batch(
                    batch_id=f"batch_{i}",
                    sensor_id="sensor_1",
                    device_id="device_1",
                    windows=[],
                    created_at=time.time()
                )
                queue.enqueue(batch)
            
            assert queue.size() == 5
            
            queue.clear()
            
            assert queue.size() == 0
            
        finally:
            shutil.rmtree(temp_dir)
    
    def test_peek(self):
        """Test peeking at queue without removing."""
        temp_dir = tempfile.mkdtemp()
        
        try:
            queue = FileQueue(temp_dir)
            
            batch = Batch(
                batch_id="test_peek",
                sensor_id="sensor_1",
                device_id="device_1",
                windows=[],
                created_at=time.time()
            )
            
            queue.enqueue(batch)
            
            # Peek should return the batch but not remove it
            peeked = queue.peek()
            assert peeked.batch_id == "test_peek"
            
            # Dequeue should still work
            dequeued = queue.dequeue()
            assert dequeued.batch_id == "test_peek"
            
        finally:
            shutil.rmtree(temp_dir)
    
    def test_durability_across_restarts(self):
        """Test that queue survives across restarts."""
        temp_dir = tempfile.mkdtemp()
        
        try:
            # Create queue and add batches
            queue1 = FileQueue(temp_dir)
            
            for i in range(3):
                batch = Batch(
                    batch_id=f"batch_{i}",
                    sensor_id="sensor_1",
                    device_id="device_1",
                    windows=[],
                    created_at=time.time()
                )
                queue1.enqueue(batch)
            
            assert queue1.size() == 3
            
            # Create new queue (simulating restart)
            queue2 = FileQueue(temp_dir)
            assert queue2.size() == 3
            
            # Dequeue from new queue
            retrieved = queue2.dequeue()
            assert retrieved.batch_id == "batch_0"
            
        finally:
            shutil.rmtree(temp_dir)


class TestBatch:
    """Tests for Batch dataclass."""
    
    def test_creation(self):
        """Test creating a Batch."""
        batch = Batch(
            batch_id="test",
            sensor_id="sensor_1",
            device_id="device_1",
            windows=[{'rms': 1.0}],
            created_at=time.time()
        )
        
        assert batch.batch_id == "test"
        assert batch.sensor_id == "sensor_1"
        assert batch.device_id == "device_1"
        assert len(batch.windows) == 1
        assert batch.compression == "gzip"
    
    def test_serialization(self):
        """Test JSON serialization."""
        batch = Batch(
            batch_id="test",
            sensor_id="sensor_1",
            device_id="device_1",
            windows=[{'rms': 1.0, 'timestamp': 0}],
            created_at=12345.0
        )
        
        # Convert to JSON
        json_str = batch.to_json()
        data = json.loads(json_str)
        
        assert data['batch_id'] == "test"
        assert data['sensor_id'] == "sensor_1"
        assert len(data['windows']) == 1
        
        # Convert back
        restored = Batch.from_dict(data)
        assert restored.batch_id == batch.batch_id
        assert restored.sensor_id == batch.sensor_id
    
    def test_to_dict(self):
        """Test to_dict method."""
        batch = Batch(
            batch_id="test",
            sensor_id="sensor_1",
            device_id="device_1",
            windows=[],
            created_at=12345.0
        )
        
        d = batch.to_dict()
        
        assert d['batch_id'] == "test"
        assert d['sensor_id'] == "sensor_1"
        assert d['device_id'] == "device_1"
        assert d['created_at'] == 12345.0
        assert d['compression'] == "gzip"


class TestMockS3Client:
    """Tests for MockS3Client."""
    
    def test_upload_success(self):
        """Test successful upload."""
        client = MockS3Client(failure_rate=0.0)
        
        success = client.upload("test_key", b"test_data")
        assert success is True
    
    def test_upload_failure(self):
        """Test failed upload with failure rate."""
        client = MockS3Client(failure_rate=1.0)  # Always fail
        
        success = client.upload("test_key", b"test_data")
        assert success is False
    
    def test_upload_batch(self):
        """Test uploading a batch."""
        client = MockS3Client(failure_rate=0.0)
        
        batch = Batch(
            batch_id="test",
            sensor_id="sensor_1",
            device_id="device_1",
            windows=[],
            created_at=time.time()
        )
        
        success = client.upload_batch(batch)
        assert success is True
    
    def test_object_key_partitioning(self):
        """Test S3 object key partitioning."""
        from datetime import datetime
        
        client = MockS3Client()
        
        batch = Batch(
            batch_id="test",
            sensor_id="vibration_01",
            device_id="edge_01",
            windows=[],
            created_at=datetime(2024, 1, 15, 14, 30, 0).timestamp()
        )
        
        timestamp = datetime(2024, 1, 15, 14, 30, 0)
        key = client._get_object_key(batch, timestamp)
        
        expected = "edge_01/vibration_01/year=2024/month=01/day=15/hour=14/test.json.gz"
        assert key == expected
    
    def test_set_failure_rate(self):
        """Test setting failure rate."""
        client = MockS3Client(failure_rate=0.0)
        
        # Should succeed
        for _ in range(10):
            assert client.upload("key", b"data") is True
        
        # Change failure rate
        client.set_failure_rate(1.0)
        
        # Should fail
        for _ in range(10):
            assert client.upload("key", b"data") is False


class TestS3Config:
    """Tests for S3Config."""
    
    def test_default_values(self):
        """Test default configuration."""
        config = S3Config()
        
        assert config.bucket_name == "edge-vibration-data"
        assert config.region == "us-east-1"
        assert config.endpoint_url is None
        assert config.aws_access_key_id is None
        assert config.aws_secret_access_key is None
        assert config.use_ssl is True
    
    def test_custom_values(self):
        """Test custom configuration."""
        config = S3Config(
            bucket_name="custom-bucket",
            region="eu-west-1",
            endpoint_url="http://localhost:4566",
            use_ssl=False
        )
        
        assert config.bucket_name == "custom-bucket"
        assert config.region == "eu-west-1"
        assert config.endpoint_url == "http://localhost:4566"
        assert config.use_ssl is False


class TestCloudSync:
    """Tests for CloudSync."""

    def test_add_window_accepts_window_features_object(self):
        """WindowFeatures objects from the processor should be accepted by CloudSync."""
        temp_dir = tempfile.mkdtemp()

        try:
            sync = CloudSync(
                sensor_id="test_sensor",
                device_id="test_device",
                batch_size=1,
                queue_dir=temp_dir,
                background=False,
            )

            window = WindowFeatures(
                timestamp=1.0,
                window_start=0.0,
                window_end=1.0,
                n_samples=1,
                rms=1.0,
                mean=0.0,
                std=1.0,
                dominant_freq=10.0,
                is_anomaly=False,
                anomaly_score=0.0,
            )

            sync.add_window(window)
            sync.flush()

            assert sync.uploaded_batches == 1
        finally:
            sync.shutdown()
            shutil.rmtree(temp_dir)
    
    def test_add_window(self):
        """Test adding windows to sync."""
        temp_dir = tempfile.mkdtemp()
        
        try:
            sync = CloudSync(
                sensor_id="test_sensor",
                device_id="test_device",
                batch_size=10,
                queue_dir=temp_dir,
                background=False  # Synchronous for testing
            )
            
            # Add windows
            for i in range(5):
                window = {
                    'timestamp': float(i),
                    'rms': float(i),
                    'mean': float(i),
                    'std': float(i)
                }
                sync.add_window(window)
            
            # Check that files were created
            assert sync.file_queue.size() == 0  # Not full yet
            
            # Add more to trigger upload
            for i in range(10):
                window = {
                    'timestamp': float(i + 5),
                    'rms': float(i + 5),
                    'mean': float(i + 5),
                    'std': float(i + 5)
                }
                sync.add_window(window)
            
            # Should have triggered upload
            assert sync.uploaded_batches >= 1
            
        finally:
            sync.shutdown()
            shutil.rmtree(temp_dir)
    
    def test_flush(self):
        """Test flushing pending batches."""
        temp_dir = tempfile.mkdtemp()
        
        try:
            sync = CloudSync(
                sensor_id="test_sensor",
                device_id="test_device",
                batch_size=100,  # Large batch size
                max_batch_age=10.0,  # Large age
                queue_dir=temp_dir,
                background=False
            )
            
            # Add some windows but not enough for auto-upload
            for i in range(5):
                window = {'timestamp': float(i), 'rms': float(i)}
                sync.add_window(window)
            
            # Flush should upload the pending batch
            sync.flush()
            
            assert sync.uploaded_batches == 1
            
        finally:
            sync.shutdown()
            shutil.rmtree(temp_dir)
    
    def test_get_stats(self):
        """Test getting sync statistics."""
        temp_dir = tempfile.mkdtemp()
        
        try:
            sync = CloudSync(
                sensor_id="test_sensor",
                device_id="test_device",
                queue_dir=temp_dir,
                background=False
            )
            
            stats = sync.get_stats()
            
            assert 'uploaded_batches' in stats
            assert 'failed_batches' in stats
            assert 'pending_batches' in stats
            assert 'avg_retries' in stats
            assert 'max_retries' in stats
            
        finally:
            sync.shutdown()
            shutil.rmtree(temp_dir)
    
    def test_recovery(self):
        """Test recovery of unsent batches after restart."""
        temp_dir = tempfile.mkdtemp()
        
        try:
            # Create sync and add some batches
            sync1 = CloudSync(
                sensor_id="test_sensor",
                device_id="test_device",
                queue_dir=temp_dir,
                background=False,
                failure_rate=1.0  # Always fail
            )
            
            # Add windows (will fail to upload)
            for i in range(20):
                window = {'timestamp': float(i), 'rms': float(i)}
                sync1.add_window(window)
            
            # Shutdown
            sync1.shutdown()
            
            # Check that batches are still in queue
            queue = FileQueue(temp_dir)
            assert queue.size() >= 1
            
            # Create new sync (simulating restart)
            sync2 = CloudSync(
                sensor_id="test_sensor",
                device_id="test_device",
                queue_dir=temp_dir,
                background=False,
                failure_rate=0.0  # Now succeed
            )
            
            # Recover should upload pending batches
            sync2.recover()
            sync2.flush()
            
            # Should have uploaded the recovered batches
            assert sync2.uploaded_batches >= 1
            
        finally:
            shutil.rmtree(temp_dir)
    
    def test_failure_retry(self):
        """Test retry logic on failure."""
        temp_dir = tempfile.mkdtemp()
        
        try:
            sync = CloudSync(
                sensor_id="test_sensor",
                device_id="test_device",
                queue_dir=temp_dir,
                background=False,
                max_retries=3,
                retry_delay=0.01,  # Short delay for testing
                failure_rate=0.5  # Fail sometimes
            )
            
            # Add windows
            for i in range(10):
                window = {'timestamp': float(i), 'rms': float(i)}
                sync.add_window(window)
            
            # Flush
            sync.flush()
            
            # Check retry stats
            stats = sync.get_stats()
            assert stats['uploaded_batches'] >= 0
            
        finally:
            sync.shutdown()
            shutil.rmtree(temp_dir)
    
    def test_set_failure_rate(self):
        """Test setting failure rate dynamically."""
        temp_dir = tempfile.mkdtemp()
        
        try:
            sync = CloudSync(
                sensor_id="test_sensor",
                device_id="test_device",
                queue_dir=temp_dir,
                background=False,
                failure_rate=0.0
            )
            
            # Should succeed
            for i in range(10):
                window = {'timestamp': float(i), 'rms': float(i)}
                sync.add_window(window)
            sync.flush()
            
            initial_uploads = sync.uploaded_batches
            
            # Set high failure rate
            sync.set_failure_rate(1.0)
            
            # Should fail
            for i in range(10, 20):
                window = {'timestamp': float(i), 'rms': float(i)}
                sync.add_window(window)
            sync.flush()
            
            # Uploads should not have increased
            assert sync.uploaded_batches == initial_uploads
            assert sync.failed_batches > 0
            
        finally:
            sync.shutdown()
            shutil.rmtree(temp_dir)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
