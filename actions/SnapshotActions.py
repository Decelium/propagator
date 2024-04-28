
import traceback as tb
from Snapshot import Snapshot
from Migrator import Migrator
from datasource.TpIPFSDecelium import TpIPFSDecelium
from datasource.TpIPFSLocal import TpIPFSLocal

import decelium_wallet.core as core
import ipfshttpclient
import os
import json
import pprint
import pandas
import shutil
import random

def agent_action(**overrides):
    def decorator(run_func):
        class CustomAction(Action):
            def run(self, record, memory):
                return run_func(self, record, memory)

        for name, func in overrides.items():
            setattr(CustomAction, name, func)

        return CustomAction()
    return decorator

class Action():
    def __init__(self):
        self.__memory = {}
    def __call__(self, record, memory=None):
        if memory == None:
            memory = {}
        return self.crun(record, memory)    
      
    def run(self,record,memory):
        raise Exception("Unimplemented")
        return

    def prevalid(self,record,memory):
        return True
    
    def postvalid(self,record,response,memory):
        return True
    
    def crun(self,record,memory=None):
        if memory == None:
            memory = {}
        err_str = "Unknown Error"
        try:
            assert self.prevalid(record,memory)
            response = self.run(record,memory)
            assert self.postvalid(record,response,memory)
            return response
        except Exception as e :
            # Package up a highly detailed exception log for record keeping
            exc = tb.format_exc()
            goal_text = self.explain(record,memory)
            err_str = "Encountered an exception when seeking action:\n\n "
            err_str += goal_text
            #err_str += "Exception:\n\n"
            #err_str += exc
            print(err_str)
            raise e

    def test(self):
        return True

    def explain(self,record,memory):
        return ""
    
    def generate(self,lang,record,memory):
        return ""
    
class ExampleAction(Action):
    def run(self,record,memory):
        raise Exception("Unimplemented")
        return
    def prevalid(self,record,memory):
        return True
    def postvalid(self,record,response,memory):
        return True
    def explain(self,record,memory):
        return ""
    def test(self):
        return True
    def generate(self,lang,record,memory):
        return ""    

import decelium_wallet.core as core

class CreateDecw(Action):
    def run(self,record,memory):
        decw = core()
        with open(record['wallet_path'],'r') as f:
            data = f.read()
        with open(record['wallet_password_path'],'r') as f:
            password = f.read()
        loaded = decw.load_wallet(data,password)
        assert loaded == True
        connected = decw.initial_connect(target_url=record['fabric_url'],
                                        api_key=decw.dw.pubk())
        return decw,connected

    def prevalid(self,record,memory):
        assert 'wallet_path' in record
        assert 'wallet_password_path' in record
        assert 'fabric_url' in record        

        return True
    
    def postvalid(self,record,response,memory=None):
        assert type(response[0]) == core
        assert response[1] == True
        return True
   
    def test(self,record):
        return True

    def explain(self,record,memory=None):
        result = '''
        CreateDecw

        Standard initialization work for decelium. This code loads a wallet from a path, 
        and establishes a connection with a miner. If it succeeds, it means a wallet was indeed
        loaded, and that a connection to a local or remote miner has been established.

        '''
        return result
    
    def generate(self,lang,record,memory=None):
        return ""
    


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

        assert TpIPFSDecelium.ipfs_has_cids(record['decw'],new_cids, record['connection_settings']) == True
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
        obj = TpIPFSDecelium.load_entity({'api_key':'UNDEFINED',"self_id":self_id,'attrib':True},decw)

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

class CorruptObject(Action):    
    def explain(self,record,memory):
        return """
        CorruptObject

        This is an action which purposely corrupts a local file backup. This is to simulate various corruption methods
        such that the file can be restored and validated afterward. The complete version of this process ensures
        a) pre: The backup is complete before corruption
        b) complete a corruption
        c) post: The corruption is reported correctly by the validation tools
         ['delete_payload','remove_attrib','rename_attrib_filename']
        """
    @staticmethod
    def corrupt_remote_corrupt_payload(record,memory):
        backup_path = record['backup_path']
        self_id = record['obj_id']
        decw = record['decw']
        connection_settings = record['connection_settings']
        obj = decw.net.corrupt_entity(decw.dw.sr({'self_id':self_id,'api_key':decw.dw.pubk(),"corruption":"delete_payload"},["admin"]))
        pins = decw.net.download_pin_status({
                'api_key':"UNDEFINED",
                'do_refresh':True,
                'connection_settings':connection_settings})    

    @staticmethod
    def corrupt_remote_remove_attrib(record,memory):
        backup_path = record['backup_path']
        self_id = record['obj_id']
        decw = record['decw']
        connection_settings = record['connection_settings']
        obj = decw.net.corrupt_entity(decw.dw.sr({'self_id':self_id,'api_key':decw.dw.pubk(),"corruption":"remove_attrib"},["admin"]))

    @staticmethod
    def corrupt_remote_rename_attrib_filename(record,memory):
        backup_path = record['backup_path']
        self_id = record['obj_id']
        decw = record['decw']
        connection_settings = record['connection_settings']

        success = decw.net.corrupt_entity(decw.dw.sr({'self_id':self_id,'api_key':decw.dw.pubk(),"corruption":"rename_attrib_filename"},["admin"]))
        assert success == True
    @staticmethod
    def corrupt_remote_delete_payload(record,memory):
        backup_path = record['backup_path']
        self_id = record['obj_id']
        decw = record['decw']
        connection_settings = record['connection_settings']

        obj = TpIPFSDecelium.load_entity({'self_id':self_id,'api_key':decw.dw.pubk(),"attrib":True},decw)

        cids = [obj['settings']['ipfs_cid']]
        if 'ipfs_cids' in obj['settings']:
            for cid in obj['settings']['ipfs_cids'].values():
                cids.append(cid)

        result:dict = decw.net.remove_ipfs({
                'api_key':"UNDEFINED",
                'file_type':'ipfs', 
                'connection_settings':connection_settings,
                'payload_type':'cid',
                'payload':cids})
        
        for r in result.values():
            assert r['removed'] == True
        
        for r in result.values():
            result_verify = decw.net.check_pin_status({
                    'api_key':"UNDEFINED",
                    'do_refresh':True,
                    'connection_settings':connection_settings,
                    'cid': r['cid']})
            break

        for r in result.values():
            result_verify = decw.net.check_pin_status({
                    'api_key':"UNDEFINED",
                    'connection_settings':connection_settings,
                    'cid': r['cid']})
            assert result_verify == False
    
    @staticmethod
    def corrupt_local_delete_payload(record,memory):
        backup_path = record['backup_path']
        self_id = record['obj_id']
        for filename in os.listdir(os.path.join(backup_path,self_id)):
            if filename.endswith('.dag') or filename.endswith('.file'):
                file_path = os.path.join(backup_path,self_id, filename)
                os.remove(file_path)
                memory['removed'].append(file_path)
        
    @staticmethod
    def corrupt_local_remove_attrib(record,memory):
        backup_path = record['backup_path']
        self_id = record['obj_id']
        file_path = os.path.join(backup_path, self_id, 'object.json')
        os.remove(file_path)
        memory['removed'].append(file_path)

    @staticmethod
    def corrupt_local_corrupt_attrib(record,memory):
        backup_path = record['backup_path']
        self_id = record['obj_id']
        file_path = os.path.join(backup_path, self_id, 'object.json')
        random_bytes_size = 1024
        random_bytes = random.getrandbits(8 * random_bytes_size).to_bytes(random_bytes_size, 'little')
        with open(file_path, 'wb') as corrupt_file:
            corrupt_file.write(random_bytes)
        memory['corrupted'].append(file_path)

    @staticmethod
    def corrupt_local_rename_attrib_filename(record,memory):
        backup_path = record['backup_path']
        self_id = record['obj_id']
        file_path = os.path.join(backup_path, self_id, 'object.json')
        with open(file_path, 'r') as f:
            correct_json = json.loads(f.read())
        correct_json['dir_name'] = "corrupt_name"
        with open(file_path, 'w') as f:
            f.write(json.dumps(correct_json))
        
    @staticmethod
    def corrupt_local_corrupt_payload(record,memory):
        backup_path = record['backup_path']
        self_id = record['obj_id']
        for filename in os.listdir(os.path.join(backup_path, self_id)):
            if  filename.endswith('.file'): # filename.endswith('.dag') or
                file_path = os.path.join(backup_path, self_id, filename)
                random_bytes_size = 1024
                random_bytes = random.getrandbits(8 * random_bytes_size).to_bytes(random_bytes_size, 'little')
                with open(file_path, 'wb') as corrupt_file:
                    corrupt_file.write(random_bytes)
                memory['corrupted'].append(file_path)    

    def run_corruption(self,mode: str, corruption: str, record: dict, memory: dict):
        method_name = "corrupt_" + mode + "_" + corruption
        method = getattr(CorruptObject, method_name, None)
        if method:
            method(record, memory)
        else:
            raise Exception(f"Method {method_name} not found.")

    def prevalid(self,record,memory):
        backup_path = record['backup_path']
        self_id = record['obj_id']
        connection_settings = record['connection_settings']
        decw = record['decw']
        mode = record['mode']
        assert mode in ['local','remote']
        local_results,messages = Snapshot.object_validation_status(decw,self_id,backup_path,connection_settings,mode)
        assert local_results[mode] == True
        return True

    def run(self,record,memory):
        backup_path = record['backup_path']
        self_id = record['obj_id']
        memory['removed'] = []
        memory['corrupted'] = []
        corruption = record['corruption']
        mode = record['mode']
        assert corruption in ['delete_payload','corrupt_payload','remove_attrib','corrupt_attrib','rename_attrib_filename']
        self.run_corruption(mode, corruption, record, memory)
        return True 

    def postvalid(self,record,response,memory):
        backup_path = record['backup_path']
        self_id = record['obj_id']
        connection_settings = record['connection_settings']
        decw = record['decw']
        mode = record['mode']
        local_results,messages = Snapshot.object_validation_status(decw,self_id,backup_path,connection_settings,mode)

        assert local_results[mode] == False
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
        obj = TpIPFSDecelium.load_entity({'path':path,'api_key':decw.dw.pubk(),"attrib":True},decw)
        old_cids = [obj['settings']['ipfs_cid']] 
        for old_cid in obj['settings']['ipfs_cids'].values():
            old_cids.append(old_cid)
        assert TpIPFSDecelium.ipfs_has_cids(decw,old_cids, connection_settings) == True
        memory['old_cids'] = old_cids
        return True

    def run(self,record,memory):
        decw = record['decw']
        user_context = record['user_context']
        connection_settings = record['connection_settings']
        path = record['path']
        singed_req = decw.dw.sr({**user_context, **{ 'path':path}})
        del_try = decw.net.delete_entity(singed_req)
        assert del_try == True
        return del_try

    def postvalid(self,record,response,memory):
        decw = record['decw']
        user_context = record['user_context']
        connection_settings = record['connection_settings']

        # B TODO - Check IPFS to validate the files are gone
        assert TpIPFSDecelium.ipfs_has_cids(decw, memory['old_cids'], connection_settings,refresh=True) == False
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
        #new_cids = record['new_cids']
        user_context = record['user_context']
        obj_id = record['obj_id']
        
        results = Snapshot.push_to_remote(decw, connection_settings, backup_path,limit=100, offset=0)
        obj = TpIPFSDecelium.load_entity({'api_key':'UNDEFINED',"self_id":obj_id,'attrib':True},decw)

        assert results[obj_id][0] == True
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
