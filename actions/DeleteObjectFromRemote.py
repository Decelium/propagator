
try:
    #from ..Snapshot import Snapshot
    #from ..datasource.TpIPFSLocal import TpIPFSLocal
    #from ..Messages import ObjectMessages
    #from ..type.BaseData import BaseData,auto_c
    #from ..datasource.CorruptionData import CorruptionTestData
    from .Action import Action
except:
    #from Snapshot import Snapshot
    #from datasource.TpIPFSLocal import TpIPFSLocal
    #from Messages import ObjectMessages
    #from type.BaseData import BaseData,auto_c
    #from datasource.CorruptionData import CorruptionTestData
    from .Action import Action

class DeleteObjectFromRemote(Action):    
    def explain(self,record,memory):
        return """
        Delete Object From Remote

        Delete an object from a remote location. Verify it is removed.
        """
    
    def prevalid(self,record,memory):
        decw = record['decw']
        user_context = record['user_context']
        connection_settings = record['connection_settings']
        path = record['path']
        obj = TpIPFS.get_datasource("remote").load_entity({'path':path,'api_key':decw.dw.pubk(),"attrib":True},decw)
        old_cids = [obj['settings']['ipfs_cid']] 
        for old_cid in obj['settings']['ipfs_cids'].values():
            old_cids.append(old_cid)
        assert TpIPFS.get_datasource("remote").ipfs_has_cids(decw,old_cids, connection_settings) == True
        memory['old_cids'] = old_cids
        return True

    def run(self,record,memory):
        decw = record['decw']
        user_context = record['user_context']
        connection_settings = record['connection_settings']
        path = record['path']
        singed_req = decw.dw.sr({**user_context, **{ 'path':path}})
        del_try = decw.net.delete_entity(singed_req)
        assert del_try == True, "Could not delete the entry with "+ str(singed_req) + " and result " + str(del_try) 
        return del_try

    def postvalid(self,record,response,memory):
        decw = record['decw']
        user_context = record['user_context']
        connection_settings = record['connection_settings']

        assert TpIPFS.get_datasource("remote").ipfs_has_cids(decw, memory['old_cids'], connection_settings,refresh=True) == False
        return True