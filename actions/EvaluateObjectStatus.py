try:
    from ..Snapshot import Snapshot
    #from ..datasource.TpIPFSDecelium import TpIPFSDecelium
    #from ..datasource.TpIPFSLocal import TpIPFSLocal
    #from ..Messages import ObjectMessages
    #from ..type.BaseData import BaseData,auto_c
    #from ..datasource.CorruptionData import CorruptionTestData
    #from .Action import Action
except:
    from Snapshot import Snapshot
    #from datasource.TpIPFSDecelium import TpIPFSDecelium
    #from datasource.TpIPFSLocal import TpIPFSLocal
    #from Messages import ObjectMessages
    #from type.BaseData import BaseData,auto_c
    #from datasource.CorruptionData import CorruptionTestData
    from .Action import Action,agent_action

@agent_action(
    explain=lambda self, record, memory=None: """
    Simply a status check to make sure the local object and remote object have a certain respective status. We have
    - source['local'/'remote'] = ['complete','payload_missing','payload_corrupt','object_missing','object_corrupt']
    """    
)    
def evaluate_object_status(self,record,memory=None):
    assert 'backup_path' in record
    assert 'self_id' in record
    assert 'decw' in record
    assert 'target' in record and record['target'] in ['local','remote','remote_mirror']
    assert 'status' in record
    for status in record['status']:
        assert status in ['complete','payload_missing','payload_corrupt','object_missing','object_corrupt'] 
    if record['target'] == 'local' and 'complete' in record['status']:
        results,messages = Snapshot.object_validation_status(record['decw'],record['self_id'],record['backup_path'],record['connection_settings'],'local')
        assert results['local'] == True
        return True
    elif record['target'] == 'local':
        results,messages = Snapshot.object_validation_status(record['decw'],record['self_id'],record['backup_path'],record['connection_settings'],'local')
        # print("What is this error")
        print("\nThe results:",results)
        assert results['local'] == False, "Results were invalid "+str(results)
        return True
    
    if record['target'] == 'remote_mirror' and 'complete' in record['status'] :
        results,messages = Snapshot.object_validation_status(record['decw'],record['self_id'],record['backup_path'],record['connection_settings'],'remote_mirror')
        assert results['remote_mirror'] == True, "Got an invalid REMOTE_MIRROR object_validation_status: "+str(results) + " " + str(messages)
        return True
    
    elif record['target'] == 'remote_mirror':
        results,messages = Snapshot.object_validation_status(record['decw'],record['self_id'],record['backup_path'],record['connection_settings'],'remote_mirror')
        assert results['remote_mirror'] == False
        return True

    if record['target'] == 'remote' and 'complete' in record['status'] :
        print("EXPECTING REMOTE TO BE TRUE")
        results,messages = Snapshot.object_validation_status(record['decw'],record['self_id'],record['backup_path'],record['connection_settings'],'remote')
        assert results['remote'] == True, "Got an invalid REMOTE object_validation_status: "+str(results) + " " + str(messages)
        return True
    
    elif record['target'] == 'remote':
        print("EXPECTING REMOTE TO BE FALSE")
        results,messages = Snapshot.object_validation_status(record['decw'],record['self_id'],record['backup_path'],record['connection_settings'],'remote')
        assert results['remote'] == False
        return True
    '''
    Notes for further checks:

    obj = Snapshot.load_entity({'self_id':obj_id, 'attrib':True},backup_path)
    assert 'self_id' in obj
    
    '''