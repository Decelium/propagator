try:
    from ..Snapshot import Snapshot
    #from ..datasource.TpIPFSDecelium import TpIPFSDecelium
    #from ..datasource.TpIPFSLocal import TpIPFSLocal
    #from ..Messages import ObjectMessages
    #from ..type.BaseData import BaseData,auto_c
    #from ..datasource.CorruptionData import CorruptionTestData
    from .Action import Action
except:
    from Snapshot import Snapshot
    #from datasource.TpIPFSDecelium import TpIPFSDecelium
    #from datasource.TpIPFSLocal import TpIPFSLocal
    #from Messages import ObjectMessages
    #from type.BaseData import BaseData,auto_c
    #from datasource.CorruptionData import CorruptionTestData
    from .Action import Action

class PullObjectFromRemote(Action):    
    def run(self,record,memory=None):
        decw = record['decw']
        connection_settings = record['connection_settings']
        backup_path = record['backup_path']
        obj_id = record['obj_id']
        overwrite = record['overwrite']
        expected_result = record['expected_result']

        results = Snapshot.pull_from_remote(decw, connection_settings, backup_path,limit=10, offset=0,overwrite=overwrite)
        
        assert obj_id in results
        assert results[obj_id]['local'] == expected_result
        if expected_result == True:
            assert len(results[obj_id]['local_message']) == 0 
        if expected_result == False:
            assert len(results[obj_id]['local_message']) > 0 

        obj = Snapshot.load_entity({'self_id':record['obj_id'], 'attrib':True},
                                    record['backup_path'])
        
        new_cids = [obj['settings']['ipfs_cid']] 
        for new_cid in obj['settings']['ipfs_cids'].values():
            new_cids.append(new_cid)
        return obj,new_cids 