import decelium_wallet.core as core
import ipfshttpclient
import os
import json
import pprint
from Migrator import Migrator
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
    found = Migrator.find_all_cids(decw,0,100)
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
    Migrator.download_ipfs_data(found, './test/testbackup',connection_settings)
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
        if Migrator.is_directory(decw,connection_settings,pin['cid']):
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
        if not Migrator.is_directory(decw,connection_settings,pin['cid']):
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

    all_cids = Migrator.find_all_cids(decw)
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
    Migrator.download_object(decw,obj_id, download_obj_path, connection_settings)
    

    # -- Remove Object --
    singed_req = decw.dw.sr({**user_context, **{
            'path':'temp/test_folder.ipfs'}})
    del_res = decw.net.delete_entity(singed_req)
    assert del_res == True
    # -- Verify Download Object Fails --
    obj = decw.net.download_entity( {**user_context, **{'self_id':obj_id,'attrib':True}})
    assert 'error' in obj

    
    # -- Verify Pin Missing --
    all_cids = Migrator.find_all_cids(decw)
    df = pandas.DataFrame(all_cids)
    all_cids = list(df['cid'])
    is_subset = set(new_cids) <= set(all_cids)
    assert is_subset == False
    # -- Verify CIDS off of IPFS
    # TODO -- Verify the CIDS are also off IPFS
    
    # -- Upload IPFS only Backup & Verify Correctness -- 
    cids_reuploaded =  Migrator.upload_ipfs_data(decw,download_obj_path+'/'+obj_id,connection_settings) 
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
    all_cids = Migrator.find_all_cids(decw)
    df = pandas.DataFrame(all_cids)
    all_cids = list(df['cid'])
    is_subset = set(new_cids) <= set(all_cids)
    assert is_subset == True

from actions.SnapshotActions import CreateDecw,Action,agent_action

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
        assert len(res) > 0
        print( res[record['obj_id']])
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

        assert Migrator.ipfs_has_cids(record['decw'],new_cids, record['connection_settings']) == True
        assert obj['dir_name'] == "test_folder.ipfs"
        return True

class PullObjectFromRemote(Action):    
    def run(self,record,memory=None):
        decw = record['decw']
        connection_settings = record['connection_settings']
        backup_path = record['backup_path']
        obj_id = record['obj_id']
        overwrite = record['overwrite']
        expected_result = record['expected_result']

        results = Snapshot.pull_from_remote(decw, connection_settings, backup_path,limit=10, offset=0,overwrite=overwrite)
        print(results)
        
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



class ChangeRemoteObjectName(Action):
    def explain(self,record,memory):
        pass
    def prevalid(self,record,memory):
        return True
    def postvalid(self,record,response,memory):
        decw = record['decw']
        user_context = record['user_context']
        self_id = record['self_id']
        dir_name = record['dir_name']
        assert response == True
        obj = decw.net.download_entity({'api_key':'UNDEFINED','self_id':self_id,'attrib':True})
        # pprint.pprint(obj)
        assert obj['dir_name'] == dir_name   
        return True

    def run(self,record,memory):
        # TEST CASE: Corrupt the data ChangeRemoteObjectName
        decw = record['decw']
        user_context = record['user_context']
        decw = record['decw']
        dir_name = record['dir_name']
        self_id = record['self_id']

        singed_req = decw.dw.sr({**user_context, ## 
                'self_id':self_id,
                'attrib':{'dir_name':dir_name}
                })
        edit_try = decw.net.edit_entity(singed_req)
        return edit_try
    
class CorruptLocalObjectBackup(Action):    
    def explain(self,record,memory):
        return """
        CorruptLocalObjectBackup

        This is an action which purposely corrupts a local file backup. This is to simulate various corruption methods
        such that the file can be restored and validated afterward. The complete version of this process ensures
        a) pre: The backup is complete before corruption
        b) complete a corruption
        c) post: The corruption is reported correctly by the validation tools
        """
    
    def prevalid(self,record,memory):
        backup_path = record['backup_path']
        self_id = record['obj_id']
        connection_settings = record['connection_settings']
        decw = record['decw']
        local_results,messages = Snapshot.object_validation_status(decw,self_id,backup_path,connection_settings,'local')
        assert local_results['local'] == True
        return True

    def run(self,record,memory):
        backup_path = record['backup_path']
        self_id = record['obj_id']
        memory['removed'] = []
        memory['corrupted'] = []
        corruption = record['corruption']
        assert corruption in ['delete_payload','corrupt_payload','remove_attrib','corrupt_attrib','rename_attrib_filename']
        if corruption == 'delete_payload':
            for filename in os.listdir(os.path.join(backup_path,self_id)):
                if filename.endswith('.dag') or filename.endswith('.file'):
                    file_path = os.path.join(backup_path,self_id, filename)
                    os.remove(file_path)
                    memory['removed'].append(file_path)
        
        if corruption == 'remove_attrib':
            file_path = os.path.join(backup_path, self_id, 'object.json')
            os.remove(file_path)
            memory['removed'].append(file_path)

        if corruption == 'corrupt_attrib':
            file_path = os.path.join(backup_path, self_id, 'object.json')
            random_bytes_size = 1024
            random_bytes = random.getrandbits(8 * random_bytes_size).to_bytes(random_bytes_size, 'little')
            with open(file_path, 'wb') as corrupt_file:
                corrupt_file.write(random_bytes)
            memory['corrupted'].append(file_path)

        if corruption == 'rename_attrib_filename':
            file_path = os.path.join(backup_path, self_id, 'object.json')
            with open(file_path, 'r') as f:
                correct_json = json.loads(f.read())
            correct_json['dir_name'] = "corrupt_name"
            with open(file_path, 'w') as f:
                f.write(json.dumps(correct_json))


        if corruption == 'corrupt_payload':
            for filename in os.listdir(os.path.join(backup_path, self_id)):
                if  filename.endswith('.file'): # filename.endswith('.dag') or
                    file_path = os.path.join(backup_path, self_id, filename)
                    random_bytes_size = 1024
                    random_bytes = random.getrandbits(8 * random_bytes_size).to_bytes(random_bytes_size, 'little')
                    with open(file_path, 'wb') as corrupt_file:
                        corrupt_file.write(random_bytes)
                    memory['corrupted'].append(file_path)

        return True 

    def postvalid(self,record,response,memory):
        backup_path = record['backup_path']
        self_id = record['obj_id']
        connection_settings = record['connection_settings']
        decw = record['decw']
        local_results,messages = Snapshot.object_validation_status(decw,self_id,backup_path,connection_settings,'local')
        assert local_results['local'] == False
        #assert len(memory['removed']) > 0
        if 'removed' in memory:
            for file_path in memory['removed']:
                assert os.path.exists(file_path) == False
        if 'corrupted' in memory:
            for file_path in memory['corrupted']:
                assert os.path.exists(file_path) == True
        return True


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
        obj = decw.net.download_entity({'path':path,'api_key':decw.dw.pubk(),"attrib":True})
        old_cids = [obj['settings']['ipfs_cid']] 
        for old_cid in obj['settings']['ipfs_cids'].values():
            old_cids.append(old_cid)
        assert Migrator.ipfs_has_cids(decw,old_cids, connection_settings) == True
        memory['old_cids'] = old_cids
        return True

    def run(self,record,memory):
        decw = record['decw']
        user_context = record['user_context']
        connection_settings = record['connection_settings']
        path = record['path']

        singed_req = decw.dw.sr({**user_context, **{
                'path':path}})
        del_try = decw.net.delete_entity(singed_req)
        assert del_try == True
        return del_try

    def postvalid(self,record,response,memory):
        decw = record['decw']
        user_context = record['user_context']
        connection_settings = record['connection_settings']

        # B TODO - Check IPFS to validate the files are gone
        assert Migrator.ipfs_has_cids(decw, memory['old_cids'], connection_settings) == False
        return True


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
        new_cids = record['new_cids']
        user_context = record['user_context']
        obj_id = record['obj_id']

        
        results = Snapshot.push_to_remote(decw, connection_settings, backup_path,limit=100, offset=0)
        assert results[obj_id][0] == True
        assert Migrator.ipfs_has_cids(decw,new_cids, connection_settings) == True
        obj = decw.net.download_entity({'api_key':'UNDEFINED','self_id':obj_id,'attrib':True})
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
            assert post['remote'] == True
            assert post['local'] == True

        return True
    def test(self):
        return True
    def generate(self,lang,record,memory):
        return ""  

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

    singed_req = decw.dw.sr({**user_context, **{
            'path':record['decelium_path'],
            'file_type':'ipfs',
            'payload_type':'ipfs_pin_list',
            'payload':pins}})
    obj_id = decw.net.create_entity(singed_req)
    assert 'obj-' in obj_id    



    #assert Migrator.ipfs_has_cids(decw,new_cids, record['ipfs_req_context']['connection_settings']) == False    
    #new_cids = [response['settings']['ipfs_cid']] 
    #for new_cid in response['settings']['ipfs_cids'].values():
    #    new_cids.append(new_cid)
    #assert Migrator.ipfs_has_cids(record['decw'],new_cids, record['connection_settings']) == True

    return obj_id


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
    assert 'target' in record and record['target'] in ['local','remote']
    assert 'status' in record
    for status in record['status']:
        assert status in ['complete','payload_missing','payload_corrupt','object_missing','object_corrupt'] 
    if record['target'] == 'local' and 'complete' in record['status']:
        results,messages = Snapshot.object_validation_status(record['decw'],record['self_id'],record['backup_path'],record['connection_settings'],'local')
        assert results['local'] == True
        return True
    elif record['target'] == 'local':
        results,messages = Snapshot.object_validation_status(record['decw'],record['self_id'],record['backup_path'],record['connection_settings'],'local')
        assert results['local'] == False
        return True

    if record['target'] == 'remote' and 'complete' in record['status'] :
        results,messages = Snapshot.object_validation_status(record['decw'],record['self_id'],record['backup_path'],record['connection_settings'],'remote')
        assert results['remote'] == True
        return True
    elif record['target'] == 'remote':
        results,messages = Snapshot.object_validation_status(record['decw'],record['self_id'],record['backup_path'],record['connection_settings'],'remote')
        assert results['remote'] == False
        return True
    '''
    Notes for further checks:

    obj = Snapshot.load_entity({'self_id':obj_id, 'attrib':True},backup_path)
    assert 'self_id' in obj
    
    '''

def test_simple_snapshot():
    # setup connection 
    create_wallet_action = CreateDecw()
    append_object_from_remote = AppendObjectFromRemote()
    delete_object_from_remote = DeleteObjectFromRemote()
    push_from_snapshot_to_remote = PushFromSnapshotToRemote()
    corrupt_local_object_backup = CorruptLocalObjectBackup()
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
        'new_cids':new_cids,
    })
    evaluate_object_status({**eval_context,'target':'local','status':['complete']})
    evaluate_object_status({**eval_context,'target':'remote','status':['complete']})    

    local_corruptions= [
        #{'local_corruption':"delete_payload","expect":True}, # Tuesday
        #{'local_corruption':"corrupt_payload","expect":True}, # Tuesday
        # {'local_corruption':"remove_attrib","expect":True}, # Wed
        #{'local_corruption':"corrupt_attrib","expect":True}, # Wed
        {'local_corruption':"rename_attrib_filename","expect":True}, # Thurs
        ]

    for corruption in local_corruptions:
        backup_instruction  ={
            'decw': decw,
            'obj_id':obj['self_id'],
            'backup_path':backup_path,        
            'connection_settings':connection_settings,        
        }
        backup_instruction["corruption"] = corruption['local_corruption']
        backup_instruction.update(corruption)
        print("TESTING CORRUPTION")
        corrupt_local_object_backup(backup_instruction)
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
        return
    

    return
    change_remote_object_name({
        'decw': decw,
        'user_context': user_context,
        'dir_name':dir_name,
        'self_id':self_id
    })
    evaluate_object_status({**eval_context,'target':'local','status':['complete']})
    evaluate_object_status({**eval_context,'target':'remote','status':['complete']})    


    obj_updated = Snapshot.load_entity({'self_id':obj_id,'attrib':True},backup_path)
    assert obj_updated['dir_name'] == "test_folder.ipfs"

    pull_object_from_remote({
        'connection_settings':connection_settings,
        'backup_path':backup_path,
        'overwrite': True,
        'decw': decw,
        'user_context': user_context,
        'self_id':self_id
    })

    return
    # TODO -- Move into pull:
    # Validate all files present
    # Validate all names are equal
    # ETC
    assert results[obj['self_id']]['local'] == True
    assert len(results[obj['self_id']]['local_message']) == 0 
    assert len(results) == 1
    obj_updated = Snapshot.load_entity({'self_id':obj_id,'attrib':True},backup_path)
    assert obj_updated['dir_name'] == "test_folder2.ipfs"


    backedup_cids = [obj['settings']['ipfs_cid']] 
    for backedup_cid in obj['settings']['ipfs_cids'].values():
        backedup_cids.append(backedup_cid)
    local_ids = []
    for filename in os.listdir(backup_path+'/'+obj['self_id']):
        if filename.endswith('.dag') or filename.endswith('.file'):
            local_ids.append(filename.split('.')[0])
    print("Compare lists")

    print(local_ids)
    print(backedup_cids)

    assert set(local_ids) <= set(backedup_cids)
    assert set(backedup_cids) <= set(local_ids)


    singed_req = decw.dw.sr({**user_context, ## 
            'self_id':obj['self_id'],
            'attrib':{'name':"test_folder.ipfs"}})
    edit_try = decw.net.edit_entity(singed_req)
    assert edit_try == True

# TODO - All entities need a checksum system / sig system
# TODO - 
# run_ipfs_backup() 
# test_ipfs_file_backup()
# test_ipfs_folder_backup()
# test_object_backup() - 
test_simple_snapshot()