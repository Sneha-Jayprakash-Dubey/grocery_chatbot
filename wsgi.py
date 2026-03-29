"""Render/Gunicorn entrypoint.

Avoids module-name collision between root `app.py` and package directory `app/`.
"""

from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path


ROOT = Path(__file__).resolve().parent
APP_FILE = ROOT / "app.py"

spec = spec_from_file_location("root_app_module", APP_FILE)
if spec is None or spec.loader is None:
    raise RuntimeError(f"Unable to load Flask app module from {APP_FILE}")
module = module_from_spec(spec)
spec.loader.exec_module(module)

app = module.app

