
import traceback as tb
try:
    from ..Snapshot import Snapshot
    from ..datasource.TpIPFS import TpIPFS
    from .Action import Action
except:
    from Snapshot import Snapshot
    from datasource.TpIPFS import TpIPFS
    from .Action import Action

class AppendObjectFromRemote(Action):    
    def explain(self,record,memory=None):
        return """
        AppendObjectFromRemote

        A quick action that uses Snapshot to append a specific object, from remote, to local. It verifies that the resulting directory 
        exists after downloading.
        """
    
    def prevalid(self,record,memory=None):
        assert 'obj_id' in record
        assert 'decw' in record
        assert 'connection_settings' in record
        assert 'backup_path' in record
        return True

    def run(self,record,memory=None):
        filter = {'attrib':{'self_id':record['obj_id']}}
        limit = 20
        offset = 0
        res = Snapshot.append_from_remote(record['decw'], record['connection_settings'], record['backup_path'], limit, offset,filter)
        print(res)
        assert len(res) > 0
        assert res[record['obj_id']]['local'] == True
        
        obj = Snapshot.load_entity({'self_id':record['obj_id'], 'attrib':True},
                                    record['backup_path'])
        new_cids = [obj['settings']['ipfs_cid']] 
        for new_cid in obj['settings']['ipfs_cids'].values():
            new_cids.append(new_cid)
        return obj,new_cids 

    def postvalid(self,record,response,memory=None):
        obj = response[0]
        new_cids = response[1]
        
        assert TpIPFS.get_datasource("remote").ipfs_has_cids(record['decw'],new_cids, record['connection_settings']) == True
        assert obj['dir_name'] == "test_folder.ipfs"
        return True