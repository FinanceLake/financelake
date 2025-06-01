import numpy as np

def remove_outliers(data: list[float], threshold: float = 3.0) -> list[float]:
    mean = np.mean(data)
    std = np.std(data)
    return [x for x in data if abs((x - mean) / std) < threshold]


