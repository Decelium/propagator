import decelium_wallet.core as core
import pandas
try:
    from Messages import ObjectMessages
except:
    from ..Messages import ObjectMessages
import traceback as tb
import ipfshttpclient
from .TpGeneralDecelium import TpGeneralDecelium
class TpGeneralDeceliumMirror(TpGeneralDecelium):
    @classmethod
    def load_entity(cls,query,decw):
        assert 'api_key' in query
        assert 'self_id' in query
        return decw.net.download_entity_mirror(query)