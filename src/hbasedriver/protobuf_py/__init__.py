# since protoc generated py files use import directly. we have to add this path to PYTHONPATH.
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
