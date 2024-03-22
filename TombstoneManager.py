import hashlib
import json
import os

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

    def _generate_hash(self, data):
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

    def commit(self, self_id, data):
        TombstoneArchive.initalize(self_id,{"hash":"initial_commit"})

        latest_index = self.commit_len(self_id) - 1
        previous_index = latest_index-1

        is_duplicate = False
        if self.generate_hash(self_id,data,previous_index) ==  self.get_commit(self_id,latest_index)['hash']:
            is_duplicate = True
        if is_duplicate:
            return False
        commit_data = {
            "hash": self.generate_hash(self_id,data,latest_index),
        }
        TombstoneArchive.append(self_id,commit_data)
        return True

# Example usage:
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

# - All file edits maintain a tombstone
# - Tombstone record is passed back with all attrib
# - restore function exists
# - delete has function to purge all data
# - can access tombstone of deleted item, until purged