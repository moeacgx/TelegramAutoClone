import sys
from pathlib import Path
from types import ModuleType

if "qrcode" not in sys.modules:
    fake_qrcode = ModuleType("qrcode")
    fake_qrcode.make = lambda *_args, **_kwargs: None
    sys.modules["qrcode"] = fake_qrcode

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
