from data_processing.data_storage.storage_manager import StorageManager

storage = StorageManager()

# Validate Parquet
print("Valid Parquet:", storage.validate('/data/processed/parquet/', format='parquet'))
# Validate Delta
print("Valid Delta:", storage.validate('/data/processed/delta/', format='delta')) 