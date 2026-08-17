"""FlowSignal Smart Money Concepts indicator package.

This package is intentionally observation-only. It must not import broker,
execution, LIVE/PAPER, risk, cooldown, or order-management modules.
"""

from .engine import analyze_structure, detect_confirmed_swings

__all__ = ["analyze_structure", "detect_confirmed_swings"]
