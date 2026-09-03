"""Retired Binary validator worker.

The Render worker is intentionally kept idle so it performs no Deriv network
requests, relay delivery, database writes, or Neon traffic. Historical Binary
research remains available in Git history if the feature is ever restored.
"""

from __future__ import annotations

import time


def main() -> None:
    print("BINARY_FORWARD_VALIDATOR_DISABLED = True", flush=True)
    while True:
        time.sleep(3600)


if __name__ == "__main__":
    main()
