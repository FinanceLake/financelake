def min_max_normalize(data: list[float]) -> list[float]:
    min_val = min(data)
    max_val = max(data)
    return [(x - min_val) / (max_val - min_val) for x in data]

