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
from .TpGeneralLocal import TpGeneralLocal
class TpUserLocal(TpGeneralLocal):
    pass
