"""Pytest configuration: ensure project root is on sys.path so tests
can `import models`, `import database`, etc. exactly as the bot does.
"""

import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
