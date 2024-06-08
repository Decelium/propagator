import hashlib
import json
import os
import base64

class TombstoneArchive:
    @staticmethod
    def exists(repo,self_id):
        file_name = os.path.join(repo,self_id+".tombstone.json")
        if not os.path.exists(file_name):
            return False
        return True
    @staticmethod
    def initalize(repo,self_id,initial_commit):
        file_name = os.path.join(repo,self_id+".tombstone.json")
        if not os.path.exists(file_name):
            with open(file_name, "w") as file:
                json.dump([initial_commit], file, indent=4)

    @staticmethod
    def delete(repo,self_id):
        file_name = os.path.join(repo,self_id+".tombstone.json")
        if os.path.exists(file_name):
            os.remove(file_name) 
        return True
    
    @staticmethod
    def get_records(repo,self_id):
        file_name = os.path.join(repo,self_id+".tombstone.json")
        with open(file_name, "r") as file:
            commits = json.load(file)
        return commits
    @staticmethod
    def length(repo,self_id):
        file_name = os.path.join(repo,self_id+".tombstone.json")
        with open(file_name, "r") as file:
            commits = json.load(file)
        return len(commits)

    @staticmethod
    def append(repo,self_id,commit_data):
        file_name = os.path.join(repo,self_id+".tombstone.json")
        with open(file_name, "r") as file:
            existing_data = json.load(file)
        existing_data.append(commit_data)

        with open(file_name, "w") as file:
            json.dump(existing_data, file, indent=4)
            return True
        return False
    
class TombstoneManager:

    def __init__(self,repo, hash_algo='sha256'):
        self.repo = repo
        self.hash_function = getattr(hashlib, hash_algo, hashlib.sha256)

    def purge_commits(self, self_id):
        return TombstoneArchive.delete(self.repo,self_id)

    def _generate_hash(self, raw_data):
        data = self.encode_data(raw_data) # A reversable encoding
        hash_obj = self.hash_function()
        hash_obj.update(data.encode('utf-8'))
        return hash_obj.hexdigest()
    def commit_len(self, self_id,):
        return TombstoneArchive.length(self.repo,self_id)
    
    def get_commit(self, self_id,index):
        commits = TombstoneArchive.get_records(self.repo,self_id)
        if index >= len(commits):
            index = len(commits)-1
        if index < 0:
            index = 0        

        val = commits[ index ]
        return val

    def generate_hash(self, self_id, data,index):
        previous_entry = self.get_commit(self_id,index)

        commit_data = {
            "hash": previous_entry["hash"],
            "data": data,
        }
        this_hash = self._generate_hash(json.dumps(commit_data))
        return this_hash

    def decode_data(self, encoded_data):
        # Decode the base64 encoded string to bytes
        data_bytes = base64.b64decode(encoded_data)
        
        # Decode bytes to string
        decoded_string = data_bytes.decode()
        
        # Extract the prefix and the data
        method,prefix, data_string = decoded_string.split(":", 2)
        if method != "01":
            raise ValueError("Unsupported encoding mode")
        
        # Convert the data back to its original type based on the prefix
        if prefix == "str":
            return data_string
        elif prefix == "int":
            return int(data_string)
        elif prefix == "dict":
            return json.loads(data_string)
        elif prefix == "list":
            return json.loads(data_string)
        else:
            raise ValueError("Unsupported type prefix")


    def encode_data(self,raw_data,method="01"):
        # Determine the type prefix and encode the data appropriately
        
        if isinstance(raw_data, str):
            if raw_data[0] == "{"  or raw_data[0] == "[" :
                try:
                    raw_data = json.loads(raw_data)
                except:
                    pass
                
        if isinstance(raw_data, str):
            data_bytes = ("01:str:" + raw_data).encode()
        elif isinstance(raw_data, int):
            data_bytes = ("01:int:" + str(raw_data)).encode()
        elif isinstance(raw_data, dict):
            data_bytes = ("01:dict:" + json.dumps(raw_data)).encode()
        elif isinstance(raw_data, list):
            data_bytes = ("01:list:" + json.dumps(raw_data)).encode()
        else:
            raise ValueError("Unsupported data type")
        
        # Base64 encode the bytes
        encoded_data = base64.b64encode(data_bytes)
        
        # Decode the base64 encoded bytes to a string
        final_encoding = encoded_data.decode()
        return final_encoding
        
    def verify(self, self_id, data):
        if not TombstoneArchive.exists(self.repo,self_id):
            self.commit(self_id, data)       
        
        latest_index = self.commit_len(self_id) - 1
        previous_index = latest_index-1
        
        last_hash = self.get_commit(self_id,latest_index)['hash']
                
        if self.generate_hash(self_id,data,previous_index) ==  last_hash:
            return True
        return False
        
    def commit(self, self_id, data):
        TombstoneArchive.initalize(self.repo,self_id,{"hash":self.encode_data("initial_commit")})

        latest_index = self.commit_len(self_id) - 1
        previous_index = latest_index-1

        is_duplicate = False
        last_hash = self.get_commit(self_id,latest_index)['hash']
        
        
        if self.generate_hash(self_id,data,previous_index) ==  last_hash:
            is_duplicate = True
        if is_duplicate:
            return last_hash
        commit_data = {
            "hash": self.generate_hash(self_id,data,latest_index),
        }
        TombstoneArchive.append(self.repo,self_id,commit_data)
        return commit_data['hash']
'''
import sys
import pandas as pd
import unittest    
import uuid    
    
import json

sys.path.append('../../')
sys.path.append('../')
from conf.conf import conf
import pandas as pd
import requests
import matplotlib.pyplot as plt
import datetime,time
import unittest
import uuid
from propagator.TombstoneManager import TombstoneManager 


import paxdk.PaxFinancialAPI as paxdk
from conf.conf import conf

class TestsTombstone():
    def test_tombstones(self,pq = None,api_key=None,remote=True):
        manager = TombstoneManager('/app/database/tombstone/')
        manager.purge_commits("example_id")
        first= manager.commit("example_id", "This is some test data.") 
        second = manager.commit("example_id", "This is some test data.") 
        third = manager.commit("example_id", "This is some test data.2")
        assert manager.verify("example_id","This is some test data.2") == True
        assert manager.verify("example_id","This is some test data.") == False
        
        fourth= manager.commit("example_id", "This is some test data.")
        fifth =  manager.commit("example_id", "This is some test data.")
        assert manager.verify("example_id","This is some test data.") == True
        
        assert first == second
        assert second != third
        assert third != fourth
        assert fourth == fifth
        # manager.purge_commits("example_id")
        assert manager.decode_data(manager.encode_data(10) ) == 10
        assert manager.decode_data(manager.encode_data("hello") ) == "hello"
        assert manager.decode_data(manager.encode_data({'some_dict':"test"}) ) == {'some_dict':"test"}
        assert manager.decode_data(manager.encode_data([{'some_dict':"test"}]) ) == [{'some_dict':"test"}]
        
    def test_create(self,pq = None,api_key=None,remote=True):
        path = '/example/' 
        print("-------------------RUNNING TEST "+ path)
        name ='test_file_execute.py'
        edited_name = 'test_file_edited_execute.py'
        full_path =path+name 
        source_data = "Random data"
        
        # TEST BASIC UPLOAD DOWNLOAD DOWNLOAD
        for i in range(1,4):
            data  = pq.delete_entity({'api_key':api_key,'path':full_path ,},remote=remote,show_errors=True)

        fil  = pq.create_entity({'api_key':api_key,
                               'path':path,
                               'name':name,
                               'file_type':'file',
                               'payload':source_data,
                              },remote=remote,show_errors=True)
        # Ensure tomstone ex
        assert 'obj-' in fil
        self_id = fil

        
        data  = pq.download_entity({'api_key':api_key,'path':full_path , },remote=remote,show_errors=True)
        attrib  = pq.download_entity({'api_key':api_key,'path':full_path ,'attrib':True },remote=remote,show_errors=True)
        
        
        manager = TombstoneManager('/app/database/tombstone/')
        assert manager.verify(self_id,"This is some test data.2") == False
        assert manager.verify(self_id,attrib) == True
        
        assert attrib['dir_name'] == name
        
        data  = pq.edit_entity({'api_key':api_key,
                                'path':full_path, 
                                'attrib':{'dir_name':edited_name},
                               },remote=remote,show_errors=True)
        
        print("Did the edit",data)
        attrib_edited  = pq.download_entity({'api_key':api_key,'path':full_path ,'attrib':True },remote=remote,show_errors=True)
        print(attrib_edited)
        assert attrib_edited['dir_name'] == edited_name
        assert manager.verify(self_id,attrib_edited) == True
        
        data  = pq.edit_entity({'api_key':api_key,
                                'path':full_path, 
                                'attrib':{'dir_name':name},
                               },remote=remote,show_errors=True)

        attrib  = pq.download_entity({'api_key':api_key,'path':full_path ,'attrib':True },remote=remote,show_errors=True)
        assert attrib['dir_name'] == name
        
        assert manager.verify(self_id,attrib) == True        
        data  = pq.download_entity({'api_key':api_key, 'path':full_path , },remote=remote)
        
        assert data == source_data
        data  = pq.delete_entity({'api_key':api_key, 'path':full_path , },remote=remote)        
        data  = pq.download_entity({'api_key':api_key, 'path':full_path , },remote=remote)

        assert 'error' in data
        assert manager.verify(self_id,attrib) == True
        restore_fail  = pq.restore_attrib({'api_key':api_key, 'attrib':attrib_edited , },remote=remote)
        assert 'error' in restore_fail
        restore_success  = pq.restore_attrib({'api_key':api_key, 'attrib':attrib , },remote=remote)
        print(restore_success)
        
        assert 'self_id' in restore_success
        attrib  = pq.download_entity({'api_key':api_key,'path':full_path ,'attrib':True },remote=remote,show_errors=True)
        assert manager.verify(self_id,attrib) == True        
        
    

if __name__ == '__main__':
    unittest.main()
# python3 run_single_test.py
'''