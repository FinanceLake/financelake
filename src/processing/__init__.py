"""
Processing Module for Real-Time Stock Insights
"""
# from .spark_streaming_job import StockStreamingProcessor
from processing.spark_streaming_processor import StockStreamingProcessor


__all__ = ['StockStreamingProcessor']