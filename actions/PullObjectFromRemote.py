try:
    from ..Snapshot import Snapshot
    from .Action import Action
except:
    from Snapshot import Snapshot
    from .Action import Action

class PullObjectFromRemote(Action):    
    def run(self,record,memory=None):
        decw = record['decw']
        connection_settings = record['connection_settings']
        backup_path = record['backup_path']
        obj_id = record['obj_id']
        overwrite = record['overwrite']
        expected_result = record['expected_result']
        filter = {}
        results = Snapshot.pull_from_remote(decw, connection_settings, backup_path,limit=10, offset=0,overwrite=overwrite,filter=filter)
        
        assert obj_id in results, "Did not get goo results from pull operation: "+ str(results)
        assert results[obj_id]['local'] == expected_result
        if expected_result == True:
            assert len(results[obj_id]['local_message']) == 0 
        if expected_result == False:
            assert len(results[obj_id]['local_message']) > 0 

        obj = Snapshot.load_entity({'self_id':record['obj_id'], 'attrib':True},
                                    record['backup_path'])
        
        local_results,messages = Snapshot.object_validation_status(record['decw'],obj['self_id'],record['backup_path'],record['connection_settings'],'local')
        assert local_results['local'] == True,"Got some bad results "+ str(local_results) + " : " + str(messages)

        return obj,None 