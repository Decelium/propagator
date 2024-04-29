import decelium_wallet.core as core
import ipfshttpclient
import os
import json
import pprint
from Migrator import Migrator
from datasource.TpIPFSDecelium import TpIPFSDecelium
from datasource.TpIPFSLocal import TpIPFSLocal

from Snapshot import Snapshot
import pandas
import shutil
import random 
'''
Backups are likely the MOST important aspect of Decelium.
This file tests the Migrator, a small utility that is the powerhouse behind creating and restoring backup data from the
Decelium network. 

Architecture

Outer:
- Backup.py - Driver for all backups

Snapshot: Wrapper classes to manage snapshots
- Class Snapshot - Generic Snapshot type
- Class System Smapshot - Used to save / restore all data
- Class Personal Snapshot - Save / Restore all owned data
- Class Community Snapshort - Save / Restore / community data


Migrator:
- Driver class that manages data movement between data sources

Datasource:
- Datasource - base class that provides upload / download functions for any source / dest
- Network - A Decelium Network data source (dev / stage / live etc)
- Local Disk - A local hard drive data source, bound to a system path
- Memory - An in memory storage area -- used for transient storage

'''




def run_ipfs_backup():
    decw = core()
    connection_settings = {'host': "devdecelium.com",
                            'port':5001,
                            'protocol':"http"}
    connected = decw.initial_connect(target_url="https://dev.paxfinancial.ai/data/query",api_key="UNDEFINED")
    found = TpIPFSDecelium.find_all_cids(decw,0,100)
    Migrator.download_ipfs_data(found, './ipfs_backup/',connection_settings)



def test_ipfs_file_backup():
    '''
    Low level test that verifies that the IPFS Migrator is backing up IPFS *files* in a manner that is perfect down to the bit.
    '''
    # [ ] create test data 
    decw = core()
    connection_settings = {'host': "devdecelium.com",
                            'port':5001,
                            'protocol':"http"}
    connected = decw.initial_connect(target_url="https://dev.paxfinancial.ai/data/query",api_key="UNDEFINED")

    # Upload to IPFS directly, & verify
    pins = decw.net.create_ipfs({
            'api_key':"UNDEFINED",
            'file_type':'ipfs', 
            'connection_settings':connection_settings,
            'payload_type':'local_path',
            'payload':'./test/testdata/img_test_1.png'})
    assert len(pins) > 0
    assert 'cid' in pins[0]

    # Backup from IPFS locally & verify
    found = [{'cid':pins[0]['cid'],'self_id':None}]
    TpIPFSDecelium.download_ipfs_data(found, './test/testbackup',connection_settings)
    file_path = './test/testbackup/'+pins[0]['cid']+'.file'

    # Assert the file exists 
    assert os.path.exists(file_path), "The backup file does not exist."
    with open('./test/testdata/img_test_1.png', 'rb') as original_file:
        original_content = original_file.read()
    with open(file_path, 'rb') as backup_file:
        backup_content = backup_file.read()
    assert original_content == backup_content, "The contents of the files are not identical."


    # Unpin from IPFS & Verify 
    result = decw.net.remove_ipfs({
            'api_key':"UNDEFINED",
            'file_type':'ipfs', 
            'connection_settings':connection_settings,
            'payload_type':'cid',
            'payload':[pins[0]['cid'],"Abject_Failure"]})
    assert result[pins[0]['cid']]['removed'] == True
    # LS and verify no pin exists for file
    # TODO LS and verify
    # Reupload backup & Verify 
    pins_new = decw.net.create_ipfs({
            'api_key':"UNDEFINED",
            'file_type':'ipfs', 
            'connection_settings':connection_settings,
            'payload_type':'local_path',
            'payload':file_path})
    assert len(pins_new) > 0
    assert 'cid' in pins_new[0]
    assert 'cid' in pins[0]
    assert pins[0]['cid'] == pins_new[0]['cid']
    # TODO LS and verify


def test_ipfs_folder_backup():
    '''
    Low level test that verifies that the IPFS Migrator is backing up IPFS *FOLDERS* in a manner that is perfect down to the bit.
    '''
    # [ ] create test data 
    decw = core()
    connection_settings = {'host': "devdecelium.com",
                            'port':5001,
                            'protocol':"http"}
    connected = decw.initial_connect(target_url="https://dev.paxfinancial.ai/data/query",api_key="UNDEFINED")

    # Upload to IPFS directly, & verify
    pins = decw.net.create_ipfs({
            'api_key':"UNDEFINED",
            'file_type':'ipfs', 
            'connection_settings':connection_settings,
            'payload_type':'local_path',
            'payload':'./test/testdata/test_folder'})
    assert len(pins) > 0
    assert 'cid' in pins[0]
    # Backup from IPFS locally & verify
    
    found = []
    # TODO - streamline download interface
    root = {}
    for pin in pins:
        found.append({'cid':pin['cid'],'self_id':None})
        root = {'cid':pin['cid'],'self_id':None}
            
    Migrator.download_ipfs_data(found, './test/testbackup',connection_settings)
    file_path = './test/testbackup/'+pins[0]['cid']+'.file'

    # Assert all the files exist & match  -
    assert len(pins) == 6
    for pin in pins:
        if TpIPFSDecelium.is_directory(decw,connection_settings,pin['cid']):
            continue
        path_original = './test/testdata/test_folder/'+pin['name']
        path_destination = './test/testbackup/'+pin['cid']+".file"
        assert os.path.exists(path_original)
        assert os.path.exists(path_destination)
        with open(path_original, 'rb') as original_file:
            original_content = original_file.read()
        with open(path_destination, 'rb') as backup_file:
            backup_content = backup_file.read()
        assert original_content == backup_content, "The contents of the files are not identical."

        # Unpin from IPFS & Verify
        result = decw.net.remove_ipfs({
                'api_key':"UNDEFINED",
                'file_type':'ipfs', 
                'connection_settings':connection_settings,
                'payload_type':'cid',
                'payload':[pin['cid'],"Abject_Failure"]})

        assert result[pin['cid']]['removed'] == True
        
        # Reupload backup & Verify
    
        pins_new = decw.net.create_ipfs({
                'api_key':"UNDEFINED",
                'file_type':'ipfs', 
                'connection_settings':connection_settings,
                'payload_type':'local_path',
                'payload':path_destination})
        assert len(pins_new) > 0
        assert 'cid' in pins_new[0]
        assert pin['cid'] == pins_new[0]['cid']

        # result = decw.net.remove_ipfs({
        #        'api_key':"UNDEFINED",
        #        'file_type':'ipfs', 
        #        'connection_settings':connection_settings,
        #        'payload_type':'cid',
        #        'payload':[pin['cid']]})
        # assert result[pin['cid']]['removed'] == True


    for pin in pins:
        if not TpIPFSDecelium.is_directory(decw,connection_settings,pin['cid']):
            continue
        path_folder_dest = './test/testbackup/'+pin['cid']+".dag"
        folder_json = {}
        with open(path_folder_dest,'r') as f:
            folder_json = json.loads(f.read())

        # Unpin from IPFS & Verify
        result = decw.net.remove_ipfs({
                'api_key':"UNDEFINED",
                'file_type':'ipfs', 
                'connection_settings':connection_settings,
                'payload_type':'cid',
                'payload':[pin['cid'],"Abject_Failure"]})

        result = decw.net.create_ipfs({
                'api_key':"UNDEFINED",
                'file_type':'ipfs', 
                'connection_settings':connection_settings,
                'payload_type':'ipfs_pin_list',
                'payload':folder_json['Links']})
        assert result[0]['cid'] == pin['cid']
    # TODO LS and verify

def test_object_backup():
    '''
    Low level test that verifies that the IPFS Migrator is backing up Decelium Objects in a manner that is perfect down to the bit.
    '''
    # [ ] create test data 
    
    decw = core()
    with open('../.wallet.dec','r') as f:
        data = f.read()
    with open('../.wallet.dec.password','r') as f:
        password = f.read()
    loaded = decw.load_wallet(data,password)
    assert loaded == True
    user_context = {
            'api_key':decw.dw.pubk()}
    connection_settings = {'host': "devdecelium.com",
                            'port':5001,
                            'protocol':"http"}
    connected = decw.initial_connect(target_url="https://dev.paxfinancial.ai/data/query",
                                      api_key=user_context['api_key'])

    ipfs_req_context = {**user_context, **{
            'file_type':'ipfs', 
            'connection_settings':connection_settings
        }}
    # Upload to IPFS directly, & verify

    # -- Upload Object --
    pins = decw.net.create_ipfs({**ipfs_req_context, **{
            'payload_type':'local_path',
            'payload':'./test/testdata/test_folder'
     }})

    singed_req = decw.dw.sr({**user_context, **{
            'path':'temp/test_folder.ipfs'}})
    del_try = decw.net.delete_entity(singed_req)

    singed_req = decw.dw.sr({**user_context, **{
            'path':'temp/test_folder.ipfs',
            'file_type':'ipfs',
            'payload_type':'ipfs_pin_list',
            'payload':pins}})
    obj_id = decw.net.create_entity(singed_req)
    assert 'obj-' in obj_id

    # -- Download Object --
    obj = decw.net.download_entity( {**user_context, **{'self_id':obj_id,'attrib':True}})
    obj_old = obj.copy()

    assert 'settings' in obj
    assert 'ipfs_cid' in obj['settings']
    assert 'ipfs_cids' in obj['settings']
    new_cids = [obj['settings']['ipfs_cid']]
    
    # -- Verify Pin Exists --
    for cid in obj['settings']['ipfs_cids'].values():
        new_cids.append(cid)

    all_cids = TpIPFSDecelium.find_all_cids(decw)
    df = pandas.DataFrame(all_cids)
    all_cids = list(df['cid'])
    is_subset = set(new_cids) <= set(all_cids)
    assert is_subset

    # -- Download Backup --
    download_obj_path = './test/testobjbackup'
    # download_path = './test/testbackup'
    try:
        shutil.rmtree(download_obj_path)
    except:
        pass
    # Migrator.download_ipfs_data(new_cids, download_path, connection_settings)
    TpIPFSLocal.download_object(TpIPFSDecelium,decw,obj_id, download_obj_path, connection_settings)
    

    # -- Remove Object --
    singed_req = decw.dw.sr({**user_context, **{
            'path':'temp/test_folder.ipfs'}})
    del_res = decw.net.delete_entity(singed_req)
    assert del_res == True
    # -- Verify Download Object Fails --
    obj = decw.net.download_entity( {**user_context, **{'self_id':obj_id,'attrib':True}})
    assert 'error' in obj

    
    # -- Verify Pin Missing --
    all_cids = TpIPFSDecelium.find_all_cids(decw)
    df = pandas.DataFrame(all_cids)
    all_cids = list(df['cid'])
    is_subset = set(new_cids) <= set(all_cids)
    assert is_subset == False
    # -- Verify CIDS off of IPFS
    # TODO -- Verify the CIDS are also off IPFS
    
    # -- Upload IPFS only Backup & Verify Correctness -- 
    cids_reuploaded =  TpIPFSDecelium.upload_ipfs_data(decw,download_obj_path+'/'+obj_id,connection_settings) 
    assert len(cids_reuploaded) > 0
    
    #result = decw.net.remove_ipfs({
    #        'api_key':"UNDEFINED",
    #        'file_type':'ipfs', 
    #        'connection_settings':connection_settings,
    #        'payload_type':'cid',
    #        'payload':backup_cids})
    
    # -- Restore the object with object re-upload -- 
    query = Migrator.upload_object_query(decw,obj_id, download_obj_path, connection_settings)
    result = decw.net.create_entity(decw.dw.sr({**query,**user_context},["admin"]))
    assert 'obj-' in result
    obj_new = decw.net.download_entity( {**user_context, **{'self_id':result,'attrib':True}})
    assert obj_new['self_id'] == obj_old['self_id']
    assert obj_new['parent_id'] == obj_old['parent_id']
    assert obj_new['dir_name'] == obj_old['dir_name']
    assert obj_new['settings']['ipfs_cid'] == obj_old['settings']['ipfs_cid']
    assert obj_new['settings']['ipfs_name'] == obj_old['settings']['ipfs_name']
    for key in obj_new['settings']['ipfs_cids'].keys():
        assert obj_new['settings']['ipfs_cids'][key] ==   obj_old['settings']['ipfs_cids'][key]
    
    # assert 'error' in success
    #success = Migrator.upload_object(decw,obj_id, download_obj_path+'/'+obj_id, connection_settings)
    # assert success == True
    all_cids = TpIPFSDecelium.find_all_cids(decw)
    df = pandas.DataFrame(all_cids)
    all_cids = list(df['cid'])
    is_subset = set(new_cids) <= set(all_cids)
    assert is_subset == True

from actions.SnapshotActions import CreateDecw,Action,agent_action,AppendObjectFromRemote,DeleteObjectFromRemote,DeleteObjectFromRemote,PushFromSnapshotToRemote,CorruptObject,ChangeRemoteObjectName,PullObjectFromRemote,upload_directory_to_remote,evaluate_object_status

def test_simple_snapshot():
    # setup connection 
    create_wallet_action = CreateDecw()
    append_object_from_remote = AppendObjectFromRemote()
    delete_object_from_remote = DeleteObjectFromRemote()
    push_from_snapshot_to_remote = PushFromSnapshotToRemote()
    corrupt_local_object_backup = CorruptObject()
    change_remote_object_name = ChangeRemoteObjectName()
    pull_object_from_remote = PullObjectFromRemote()

    decw, connected = create_wallet_action({
         'wallet_path': '../.wallet.dec',
         'wallet_password_path':'../.wallet.dec.password',
         'fabric_url': 'https://dev.paxfinancial.ai/data/query',
        })
    
    user_context = {
            'api_key':decw.dw.pubk()
    }
    connection_settings = {'host': "devdecelium.com",
                            'port':5001,
                            'protocol':"http"
    }
    ipfs_req_context = {**user_context, **{
            'file_type':'ipfs', 
            'connection_settings':connection_settings
    }}
    decelium_path = 'temp/test_folder.ipfs'
    local_test_folder = './test/testdata/test_folder'
    # --- Remove old snapshot #
    backup_path = "./test/system_backup_test"
    try:
        shutil.rmtree(backup_path)
    except:
        pass



    obj_id = upload_directory_to_remote({
        'local_path': local_test_folder,
        'decelium_path': decelium_path,
        'decw': decw,
        'ipfs_req_context': ipfs_req_context,
        'user_context': user_context

    })
    eval_context = {
        'backup_path':backup_path,
        'self_id':obj_id,
        'connection_settings':connection_settings,
        'decw':decw}

    evaluate_object_status({**eval_context,'target':'local','status':['object_missing','payload_missing']})
    evaluate_object_status({**eval_context,'target':'remote','status':['complete']})
    obj,new_cids = append_object_from_remote({
     'decw':decw,
     'obj_id':obj_id,
     'connection_settings':connection_settings,
     'backup_path':backup_path,
    })

    evaluate_object_status({**eval_context,'target':'local','status':['complete']})
    evaluate_object_status({**eval_context,'target':'remote','status':['complete']})


    delete_object_from_remote({
        'decw':decw,
        'user_context':user_context,
        'connection_settings':connection_settings,
        'path': decelium_path,     
    })
    evaluate_object_status({**eval_context,'target':'local','status':['complete']})
    evaluate_object_status({**eval_context,'target':'remote','status':['object_missing','payload_missing']})    
    push_from_snapshot_to_remote({
        'decw': decw,
        'obj_id':obj_id,
        'user_context':user_context,
        'connection_settings':connection_settings,
        'backup_path':backup_path,
    })
    evaluate_object_status({**eval_context,'target':'local','status':['complete']})
    evaluate_object_status({**eval_context,'target':'remote','status':['complete']})    

    all_corruptions= [ 
        {'corruption':"delete_payload","expect":True, "mode":'local'}, 
        {'corruption':"corrupt_payload","expect":True, "mode":'local'}, 
        {'corruption':"remove_attrib","expect":True, "mode":'local'}, 
        {'corruption':"corrupt_attrib","expect":True, "mode":'local'}, 
        {'corruption':"rename_attrib_filename","expect":True, "mode":'local'},
        {'corruption':"delete_payload","expect":True, "mode":'remote'}, 
        {'corruption':"corrupt_payload","expect":True, "mode":'remote'}, 
        {'corruption':"remove_attrib","expect":True, "mode":'remote'},  ####
        {'corruption':"rename_attrib_filename","expect":True, "mode":'remote'}, ####
        ]

    for corruption in all_corruptions:
        backup_instruction  ={
            'decw': decw,
            'obj_id':obj['self_id'],
            'backup_path':backup_path,        
            'connection_settings':connection_settings,        
        }
        backup_instruction["corruption"] = corruption['corruption']
        backup_instruction["mode"] = corruption['mode']
        backup_instruction.update(corruption)
        corrupt_local_object_backup(backup_instruction)
        if corruption['mode'] == 'local':
            evaluate_object_status({**eval_context,'target':'local','status':['payload_missing']})
            evaluate_object_status({**eval_context,'target':'remote','status':['complete']}) 
            pull_object_from_remote({
                'connection_settings':connection_settings,
                'backup_path':backup_path,
                'overwrite': False,
                'decw': decw,
                'user_context': user_context,
                'obj_id':obj['self_id'],
                'expected_result': corruption['expect'],
            })
            evaluate_object_status({**eval_context,'target':'local','status':['complete']})
            evaluate_object_status({**eval_context,'target':'remote','status':['complete']})    

        elif corruption['mode'] == 'remote':
            evaluate_object_status({**eval_context,'target':'local','status':['complete']})
            evaluate_object_status({**eval_context,'target':'remote','status':['payload_missing']}) 
            push_from_snapshot_to_remote({
                'decw': decw,
                'obj_id':obj_id,
                'user_context':user_context,
                'connection_settings':connection_settings,
                'backup_path':backup_path,
            })
            evaluate_object_status({**eval_context,'target':'local','status':['complete']})
            evaluate_object_status({**eval_context,'target':'remote','status':['complete']})    
        else:
            assert True == False 


def test_miner_backup():
    # setup connection 
    create_wallet_action = CreateDecw()
    append_object_from_remote = AppendObjectFromRemote()
    delete_object_from_remote = DeleteObjectFromRemote()
    push_from_snapshot_to_remote = PushFromSnapshotToRemote()
    corrupt_local_object_backup = CorruptObject()
    change_remote_object_name = ChangeRemoteObjectName()
    pull_object_from_remote = PullObjectFromRemote()

    decw, connected = create_wallet_action({
         'wallet_path': '../.wallet.dec',
         'wallet_password_path':'../.wallet.dec.password',
         'fabric_url': 'https://dev.paxfinancial.ai/data/query',
        })
    
    user_context = {
            'api_key':decw.dw.pubk()
    }
    connection_settings = {'host': "devdecelium.com",
                            'port':5001,
                            'protocol':"http"
    }
    ipfs_req_context = {**user_context, **{
            'file_type':'ipfs', 
            'connection_settings':connection_settings
    }}
    decelium_path = 'temp/test_folder.ipfs'
    local_test_folder = './test/testdata/test_folder'
    # --- Remove old snapshot #
    backup_path = "./test/system_backup_test"
    try:
        shutil.rmtree(backup_path)
    except:
        pass



    obj_id = upload_directory_to_remote({
        'local_path': local_test_folder,
        'decelium_path': decelium_path,
        'decw': decw,
        'ipfs_req_context': ipfs_req_context,
        'user_context': user_context

    })
    eval_context = {
        'backup_path':backup_path,
        'self_id':obj_id,
        'connection_settings':connection_settings,
        'decw':decw}

    evaluate_object_status({**eval_context,'target':'local','status':['object_missing','payload_missing']})
    evaluate_object_status({**eval_context,'target':'remote','status':['complete']})
    obj,new_cids = append_object_from_remote({
     'decw':decw,
     'obj_id':obj_id,
     'connection_settings':connection_settings,
     'backup_path':backup_path,
    })

    evaluate_object_status({**eval_context,'target':'local','status':['complete']})
    evaluate_object_status({**eval_context,'target':'remote','status':['complete']})


    delete_object_from_remote({
        'decw':decw,
        'user_context':user_context,
        'connection_settings':connection_settings,
        'path': decelium_path,     
    })
    evaluate_object_status({**eval_context,'target':'local','status':['complete']})
    evaluate_object_status({**eval_context,'target':'remote','status':['object_missing','payload_missing']})    
    push_from_snapshot_to_remote({
        'decw': decw,
        'obj_id':obj_id,
        'user_context':user_context,
        'connection_settings':connection_settings,
        'backup_path':backup_path,
    })
    evaluate_object_status({**eval_context,'target':'local','status':['complete']})
    evaluate_object_status({**eval_context,'target':'remote','status':['complete']})    

    all_corruptions= [ 
        {'corruption':"delete_payload","expect":True, "mode":'local'}, 
        {'corruption':"corrupt_payload","expect":True, "mode":'local'}, 
        {'corruption':"remove_attrib","expect":True, "mode":'local'}, 
        {'corruption':"corrupt_attrib","expect":True, "mode":'local'}, 
        {'corruption':"rename_attrib_filename","expect":True, "mode":'local'},
        {'corruption':"delete_payload","expect":True, "mode":'remote'}, 
        {'corruption':"corrupt_payload","expect":True, "mode":'remote'}, 
        {'corruption':"remove_attrib","expect":True, "mode":'remote'},  ####
        {'corruption':"rename_attrib_filename","expect":True, "mode":'remote'}, ####
        ]

    for corruption in all_corruptions:
        backup_instruction  ={
            'decw': decw,
            'obj_id':obj['self_id'],
            'backup_path':backup_path,        
            'connection_settings':connection_settings,        
        }
        backup_instruction["corruption"] = corruption['corruption']
        backup_instruction["mode"] = corruption['mode']
        backup_instruction.update(corruption)
        corrupt_local_object_backup(backup_instruction)
        if corruption['mode'] == 'local':
            evaluate_object_status({**eval_context,'target':'local','status':['payload_missing']})
            evaluate_object_status({**eval_context,'target':'remote','status':['complete']}) 
            pull_object_from_remote({
                'connection_settings':connection_settings,
                'backup_path':backup_path,
                'overwrite': False,
                'decw': decw,
                'user_context': user_context,
                'obj_id':obj['self_id'],
                'expected_result': corruption['expect'],
            })
            evaluate_object_status({**eval_context,'target':'local','status':['complete']})
            evaluate_object_status({**eval_context,'target':'remote','status':['complete']})    

        elif corruption['mode'] == 'remote':
            evaluate_object_status({**eval_context,'target':'local','status':['complete']})
            evaluate_object_status({**eval_context,'target':'remote','status':['payload_missing']}) 
            push_from_snapshot_to_remote({
                'decw': decw,
                'obj_id':obj_id,
                'user_context':user_context,
                'connection_settings':connection_settings,
                'backup_path':backup_path,
            })
            evaluate_object_status({**eval_context,'target':'local','status':['complete']})
            evaluate_object_status({**eval_context,'target':'remote','status':['complete']})    
        else:
            assert True == False 

# TODO - All entities need a checksum system / sig system
# TODO - 
# run_ipfs_backup() 
# test_ipfs_file_backup()
# test_ipfs_folder_backup()
# test_object_backup() - 
test_simple_snapshot()
#test_miner_backup()