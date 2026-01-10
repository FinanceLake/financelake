import torch
import pyarrow.parquet as pq
import os
import glob
import numpy as np
from torch.utils.data import IterableDataset

class StockDataset(IterableDataset):
    """
    Streams stock data from Parquet files on local disk.
    Expected Schema: 'features' (Array<Float>), 'target' (Float)
    """
    def __init__(self, data_dir):
        # Look for parquet files recursively
        self.file_paths = glob.glob(os.path.join(data_dir, "*.parquet"))
        if not self.file_paths:
            self.file_paths = glob.glob(os.path.join(data_dir, "**", "*.parquet"), recursive=True)
        
        # Filter out Spark success files or crc files
        self.file_paths = [p for p in self.file_paths if p.endswith(".parquet") and "SUCCESS" not in p]
        print(f"Found {len(self.file_paths)} data files in {data_dir}")

    def __iter__(self):
        for file_path in self.file_paths:
            try:
                parquet_file = pq.ParquetFile(file_path)
                for batch in parquet_file.iter_batches():
                    df_batch = batch.to_pandas()
                    
                    if 'features' in df_batch.columns and 'target' in df_batch.columns:
                        # Stack converts list of arrays into a 2D numpy array
                        features = np.stack(df_batch['features'].values)
                        targets = df_batch['target'].values.astype(np.float32)
                        
                        for X, y in zip(features, targets):
                            yield torch.tensor(X, dtype=torch.float32), torch.tensor([y], dtype=torch.float32)
            except Exception as e:
                print(f"Skipping corrupt file {file_path}: {e}")
                continue