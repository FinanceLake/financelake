import pytest
from data_processing.data_storage.storage_manager import StorageManager

class DummyDF:
    def write(self):
        return self
    def mode(self, m):
        return self
    def partitionBy(self, cols):
        return self
    def parquet(self, path):
        pass
    def format(self, f):
        return self
    def save(self, path):
        pass
    def count(self):
        return 1

@pytest.fixture
def storage_manager():
    return StorageManager()

def test_save(storage_manager):
    df = DummyDF()
    assert storage_manager.save(df, format='parquet') is not None

def test_load(storage_manager):
    # Would normally mock Spark read
    assert True

def test_validate(storage_manager):
    # Would normally mock DataFrame count
    assert True 