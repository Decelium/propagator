
try:
    from ..Snapshot import Snapshot
    from ..datasource.TpIPFSDecelium import TpIPFSDecelium
    #from ..datasource.TpIPFSLocal import TpIPFSLocal
    #from ..Messages import ObjectMessages
    #from ..type.BaseData import BaseData,auto_c
    #from ..datasource.CorruptionData import CorruptionTestData
    from .Action import Action
except:
    from Snapshot import Snapshot
    from datasource.TpIPFSDecelium import TpIPFSDecelium
    #from datasource.TpIPFSLocal import TpIPFSLocal
    #from Messages import ObjectMessages
    #from type.BaseData import BaseData,auto_c
    #from datasource.CorruptionData import CorruptionTestData
    from .Action import Action,agent_action

class PushFromSnapshotToRemote(Action):
    def explain(self,record,memory):
        return """PushFromSnapshotToRemote
        Given a local object within a snapshot, ensure the push operation is working correctly. 
        Push only updates the remote when the local version is up to date, while the remote is missing or incomplete. Thus
        this action must validate any circumstance.
        """

    def prevalid(self,record,memory):
        decw = record['decw']
        connection_settings = record['connection_settings']
        backup_path = record['backup_path']
        self_id = record['obj_id']
        local_results,messages = Snapshot.object_validation_status(decw,self_id,backup_path,connection_settings,'local')
        remote_results,messages = Snapshot.object_validation_status(decw,self_id,backup_path,connection_settings,'remote')

        memory['pre_obj_status'] = {**local_results,**remote_results}
        assert 'local' in memory['pre_obj_status'] and memory['pre_obj_status']['local'] in [True,False]
        assert 'remote' in memory['pre_obj_status']  and memory['pre_obj_status']['remote'] in [True,False]

        # We should have relevant status flags reade

        return True

    def run(self,record,memory):
        decw = record['decw']
        connection_settings = record['connection_settings']
        backup_path = record['backup_path']
        #new_cids = record['new_cids']
        user_context = record['user_context']
        obj_id = record['obj_id']
        
        results = Snapshot.push_to_remote(decw, connection_settings, backup_path,limit=100, offset=0)
        print("FINISHED PUSH TO REMOTE")
        assert results[obj_id][0] == True, "Could not validate "+ str(results)
        print("FINISHED PUSH TO REMOTE 2")

        obj = TpIPFSDecelium.load_entity({'api_key':'UNDEFINED',"self_id":obj_id,'attrib':True},decw)
        assert 'obj-' in obj['self_id']

    def postvalid(self,record,response,memory):
        decw = record['decw']
        connection_settings = record['connection_settings']
        backup_path = record['backup_path']
        self_id = record['obj_id']
        local_results,messages = Snapshot.object_validation_status(decw,self_id,backup_path,connection_settings,'local')
        remote_results,messages = Snapshot.object_validation_status(decw,self_id,backup_path,connection_settings,'remote')

        memory['post_obj_status'] = {**local_results,**remote_results}
        assert 'local' in memory['post_obj_status'] and memory['pre_obj_status']['local'] in [True,False]
        assert 'remote' in memory['post_obj_status']  and memory['pre_obj_status']['remote'] in [True,False]
        pre =  memory['pre_obj_status']
        post =  memory['post_obj_status']
        if pre['local'] == True and pre['remote'] == False:
            assert post['remote'] == True, "Could not validate the new remote results "+ str(post)
            assert post['local'] == True
        return True
    
    def test(self):
        return True
    def generate(self,lang,record,memory):
        return ""  