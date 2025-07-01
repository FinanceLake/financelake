from .parquet_storage import ParquetStorage
from .delta_storage import DeltaStorage
from .config import STORAGE_CONFIG

class StorageManager:
    """
    Abstraction for data storage operations (Parquet, Delta, etc.).
    """
    def __init__(self, config=STORAGE_CONFIG):
        self.config = config
        self.backends = {
            'parquet': ParquetStorage(config),
            'delta': DeltaStorage(config)
        }

    def save(self, df, format='parquet', **kwargs):
        """Save DataFrame in the specified format."""
        if format not in self.backends:
            raise ValueError(f"Unsupported format: {format}")
        return self.backends[format].save(df, **kwargs)

    def load(self, path, format='parquet', **kwargs):
        """Load DataFrame from the specified format."""
        if format not in self.backends:
            raise ValueError(f"Unsupported format: {format}")
        return self.backends[format].load(path, **kwargs)

    def validate(self, path, format='parquet', **kwargs):
        """Validate data at the given path."""
        if format not in self.backends:
            raise ValueError(f"Unsupported format: {format}")
        return self.backends[format].validate(path, **kwargs) 