
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
    print("The target Pins")
    print(pins)
    singed_req = decw.dw.sr({**user_context, **{
            'path':record['decelium_path']}})
    obj = decw.net.download_entity({**singed_req,'attrib':True})
    print("Obj1",obj)
    del_try = decw.net.delete_entity(singed_req)
    print("Del try",del_try)
    del_try = decw.net.delete_entity(singed_req)
    print("Del try",del_try)

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
    obj = decw.net.download_entity({**singed_req,'attrib':True})
    print("Obj2",obj)
    obj_id = decw.net.create_entity(singed_req)
    assert 'obj-' in obj_id," COuld not create object "+str(obj_id) 
    try:
        assert decw.has_entity_prefix(obj_id), "Dam, Could not even create an entity : "+str(obj_id)  
    except Exception as e:
        print("Failing Object Id" + str(obj_id))
        raise e
    return obj_id
