try:
    #from ..Snapshot import Snapshot
    #from ..datasource.TpIPFSDecelium import TpIPFSDecelium
    #from ..datasource.TpIPFSLocal import TpIPFSLocal
    #from ..Messages import ObjectMessages
    #from ..type.BaseData import BaseData,auto_c
    #from ..datasource.CorruptionData import CorruptionTestData
    from .Action import Action
except:
    #from Snapshot import Snapshot
    #from datasource.TpIPFSDecelium import TpIPFSDecelium
    #from datasource.TpIPFSLocal import TpIPFSLocal
    #from Messages import ObjectMessages
    #from type.BaseData import BaseData,auto_c
    #from datasource.CorruptionData import CorruptionTestData
    from .Action import Action

from decelium_wallet import core as core

class CreateDecw(Action):
    def run(self,record,memory=None):
        
        decw = core()
        with open(record['wallet_path'],'r') as f:
            data = f.read()
        with open(record['wallet_password_path'],'r') as f:
            password = f.read()
            password = password.strip()
        loaded = decw.load_wallet(data,password)
        assert loaded == True, "Could not load wallet " + str(loaded)
        connected = decw.initial_connect(target_url=record['fabric_url'],
                                        api_key=decw.dw.pubk())
        return decw,connected

    def prevalid(self,record,memory):
        assert 'wallet_path' in record
        assert 'wallet_password_path' in record
        assert 'fabric_url' in record        

        return True
    
    def postvalid(self,record,response,memory=None):
        assert type(response[0]) == core
        assert response[1] == True
        return True
   
    def test(self,record):
        return True

    def explain(self,record,memory=None):
        result = '''
        CreateDecw

        Standard initialization work for decelium. This code loads a wallet from a path, 
        and establishes a connection with a miner. If it succeeds, it means a wallet was indeed
        loaded, and that a connection to a local or remote miner has been established.

        '''
        return result
    
    def generate(self,lang,record,memory=None):
        return ""