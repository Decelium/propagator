
try:
    #from ..Snapshot import Snapshot
    #from ..datasource.TpIPFSDecelium import TpIPFSDecelium
    #from ..datasource.TpIPFSLocal import TpIPFSLocal
    #from ..Messages import ObjectMessages
    #from ..type.BaseData import BaseData,auto_c
    #from ..datasource.CorruptionData import CorruptionTestData
    from .Action import Action,agent_action
except:
    #from Snapshot import Snapshot
    #from datasource.TpIPFSDecelium import TpIPFSDecelium
    #from datasource.TpIPFSLocal import TpIPFSLocal
    #from Messages import ObjectMessages
    #from type.BaseData import BaseData,auto_c
    #from datasource.CorruptionData import CorruptionTestData
    from .Action import Action,agent_action

@agent_action(
    explain=lambda self, record, memory=None: """
    Uploads a directory to decelium. 
    Takes care of IPFS details, and cleaning up of resources before
    """    
)    
def upload_directory_to_remote(self,record,memory=None):
    assert 'local_path' in record
    assert 'decelium_path' in record
    assert 'decw' in record
    assert 'ipfs_req_context' in record
    assert 'user_context' in record

    ipfs_req_context = record['ipfs_req_context']
    user_context = record['user_context']
    decw = record['decw']
    # --- upload test dir, with specifc obj ---
    pins = decw.net.create_ipfs({**ipfs_req_context, **{
            'payload_type':'local_path',
            'payload':record['local_path']
    }})
    singed_req = decw.dw.sr({**user_context, **{
            'path':record['decelium_path']}})
    del_try = decw.net.delete_entity(singed_req)
    try:
        assert del_try == True  or ('error' in del_try and 'could not find' in del_try['error']), "Got an invalid response for del_try "+ str(del_try)
    except Exception as e:
        print("Failing Delete Object Id" + str(del_try))
        raise e
    singed_req = decw.dw.sr({**user_context, **{
            'path':record['decelium_path'],
            'file_type':'ipfs',
            'payload_type':'ipfs_pin_list',
            'payload':pins}})
    obj_id = decw.net.create_entity(singed_req)
    try:
        assert 'obj-' in obj_id    
    except Exception as e:
        print("Failing Object Id" + str(obj_id))
        raise e
    return obj_id
