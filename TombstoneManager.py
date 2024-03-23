import hashlib
import json
import os
import base64

class TombstoneArchive:

    @staticmethod
    def initalize(self_id,initial_commit):
        file_name = f"{self_id}.tombstone.json"
        if not os.path.exists(file_name):
            with open(file_name, "w") as file:
                json.dump([initial_commit], file, indent=4)

    @staticmethod
    def delete(self_id):
        file_name = f"{self_id}.tombstone.json"
        if os.path.exists(file_name):
            os.remove(file_name) 
        return True
    
    @staticmethod
    def get_records(self_id):
        file_name = f"{self_id}.tombstone.json"
        with open(file_name, "r") as file:
            commits = json.load(file)
        return commits
    @staticmethod
    def length(self_id):
        file_name = f"{self_id}.tombstone.json"
        with open(file_name, "r") as file:
            commits = json.load(file)
        return len(commits)

    @staticmethod
    def append(self_id,commit_data):
        file_name = f"{self_id}.tombstone.json"
        with open(file_name, "r") as file:
            existing_data = json.load(file)
        existing_data.append(commit_data)

        with open(file_name, "w") as file:
            json.dump(existing_data, file, indent=4)
            return True
        return False
    
class TombstoneManager:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(TombstoneManager, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self, hash_algo='sha256'):
        self.hash_function = getattr(hashlib, hash_algo, hashlib.sha256)

    def purge_commits(self, self_id):
        return TombstoneArchive.delete(self_id)

    def _generate_hash(self, raw_data):
        data = self.encode_data(raw_data) # A reversable encoding
        hash_obj = self.hash_function()
        hash_obj.update(data.encode('utf-8'))
        return hash_obj.hexdigest()
    def commit_len(self, self_id,):
        return TombstoneArchive.length(self_id)
    
    def get_commit(self, self_id,index):
        commits = TombstoneArchive.get_records(self_id)
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

    
    def commit(self, self_id, data):
        TombstoneArchive.initalize(self_id,{"hash":self.encode_data("initial_commit")})

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
        TombstoneArchive.append(self_id,commit_data)
        return commit_data['hash']

# Example usage:
'''
manager = TombstoneManager()
manager.purge_commits("example_id")
assert manager.commit("example_id", "This is some test data.") == True
assert manager.commit("example_id", "This is some test data.") == False
assert manager.commit("example_id", "This is some test data.2") == True
assert manager.commit("example_id", "This is some test data.") == True
assert manager.commit("example_id", "This is some test data.") == False

manager.purge_commits("example_id")
assert manager.commit("example_id", "This is some test data.") == True
assert manager.commit("example_id", "This is some test data.2") == True
assert manager.commit("example_id", "This is some test data.") == True
assert manager.commit("example_id", "This is some test data.") == False
'''
# - All file edits maintain a tombstone
# - Tombstone record is passed back with all attrib
# - restore function exists
# - delete has function to purge all data
# - can access tombstone of deleted item, until purged