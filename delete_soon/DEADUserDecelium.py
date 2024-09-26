import decelium_wallet.core as core
import os
import json
try:
    from Messages import ObjectMessages
except:
    from ..Messages import ObjectMessages
import traceback as tb
import hashlib
import shutil
import random
from .TpGeneralDecelium import TpGeneralDecelium
class TpUserDecelium(TpGeneralDecelium):
    pass
