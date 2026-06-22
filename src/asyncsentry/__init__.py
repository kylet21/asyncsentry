"""
asyncsentry - Async Event Loop Blocker Detector & Analyzer
Detects blocking calls and slow async tasks; logs results to stdout/stderr.
"""

from .monitor import AsyncSentry

__all__ = ["AsyncSentry"]
