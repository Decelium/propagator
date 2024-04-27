import decelium_wallet.core as core
import os
import json
import pprint
import pandas
from Messages import ObjectMessages
from datasource.TpIPFSDecelium import TpIPFSDecelium
from datasource.TpIPFSLocal import TpIPFSLocal
from datasource.EntityData import EntityData

import cid
import multihash
import traceback as tb
import hashlib
# - Remove all raw IPFS / Decelium code

class Migrator():
    pass