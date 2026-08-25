"""Optional distance gate for visual BOS/CHoCH structure events."""
from __future__ import annotations

import math


FIFTEEN_MINUTE_STRUCTURE_POINTS = 100


class StructureDistanceGate:
    """Keep the last accepted event level without post-filtering event output."""

    def __init__(self, *, timeframe=None, point_size=None):
        self.timeframe = str(timeframe or "").strip().lower()
        try:
            parsed_point_size = float(point_size)
        except (TypeError, ValueError):
            parsed_point_size = 0.0
        self.point_size = parsed_point_size if math.isfinite(parsed_point_size) and parsed_point_size > 0 else None
        self.minimum_distance = (
            FIFTEEN_MINUTE_STRUCTURE_POINTS * self.point_size
            if self.timeframe == "15m" and self.point_size is not None
            else 0.0
        )
        self.last_accepted_level = None

    def accept(self, candidate_level):
        try:
            candidate = float(candidate_level)
        except (TypeError, ValueError):
            return False
        if not math.isfinite(candidate):
            return False
        if self.last_accepted_level is not None and self.minimum_distance > 0:
            distance = abs(candidate - self.last_accepted_level)
            if distance + (self.point_size * 1e-7) < self.minimum_distance:
                return False
        self.last_accepted_level = candidate
        return True

    def config(self):
        return {
            "timeframe": self.timeframe or None,
            "point_size": self.point_size,
            "minimum_structure_points": (
                FIFTEEN_MINUTE_STRUCTURE_POINTS if self.timeframe == "15m" else 0
            ),
            "last_accepted_structure_level": self.last_accepted_level,
        }
