import decelium_wallet.core as core
import os
import json
try:
    from Messages import ObjectMessages
except:
    from ..Messages import ObjectMessages
import traceback as tb
import hashlib
import shutil
import random
from .DsGeneral import DsGeneral
from .DsGeneralLocal import DsGeneralLocal
#import datetime
'''
class Node:
    def __init__(self, cid):
        self.cid = cid
        self.dependencies = []
        
    def add_dependency(self, node):
        self.dependencies.append(node)

class CidTree:
    def __init__(self):
        self.nodes = {}

    def add_node(self, cid):
        if cid not in self.nodes:
            self.nodes[cid] = Node(cid)
        return self.nodes[cid]

    def add_dependency(self, cid, dependency_cid):
        node = self.add_node(cid)
        dependency_node = self.add_node(dependency_cid)
        node.add_dependency(dependency_node)

    def dfs_upload(self, node, visited, upload_sequence):
        if node.cid in visited:
            return
        visited.add(node.cid)
        for dependency in node.dependencies:
            self.dfs_upload(dependency, visited, upload_sequence)
        upload_sequence.append(node.cid)
        
    def get_upload_sequence_by_root(self, root_cid):
        visited = set()
        upload_sequence = []
        root_node = self.nodes.get(root_cid)
        if root_node:
            self.dfs_upload(root_node, visited, upload_sequence)
        return upload_sequence

    def get_upload_sequence(self):
        # Identify all nodes that are roots (i.e., no incoming dependencies)
        all_cids = set(self.nodes.keys())
        dependent_cids = set()
        for node in self.nodes.values():
            for dependency in node.dependencies:
                dependent_cids.add(dependency.cid)
        root_cids = all_cids - dependent_cids

        # Create a simulated root node
        simulated_root = Node("UNDEFINED")
        for root_cid in root_cids:
            simulated_root.add_dependency(self.nodes[root_cid])

        # Perform DFS from the simulated root
        visited = set()
        upload_sequence = []
        self.dfs_upload(simulated_root, visited, upload_sequence)

        # Remove the simulated root from the upload sequence
        upload_sequence.remove("UNDEFINED")
        return upload_sequence



class jsondateencode_local:
    def loads(dic):
        return json.loads(dic,object_hook=jsondateencode_local.datetime_parser)
    def dumps(dic):
        return json.dumps(dic,default=jsondateencode_local.datedefault)

    def datedefault(o):
        if isinstance(o, tuple):
            l = ['__ref']
            l = l + o
            return l
        if isinstance(o, (datetime.date, datetime.datetime,)):
            return o.isoformat()

    def datetime_parser(dct):
        DATE_FORMAT = '%Y-%m-%dT%H:%M:%S'
        for k, v in dct.items():
            if isinstance(v, str) and "T" in v:
                try:
                    dct[k] = datetime.datetime.strptime(v, DATE_FORMAT)
                except:
                    pass
        return dct
'''
'''
class DsLocalFilesystem(DsGeneralLocal):
    @classmethod
    def download_object(cls,TpSource,decw,object_ids,download_path,connection_settings, overwrite=False,attrib=None):
        pass

    @classmethod
    def merge_payload_from_remote(cls,TpRemote,decw,obj,download_path,connection_settings, overwrite):
        pass
    
    @classmethod        
    def merge_attrib_from_remote(cls,TpSource,decw,obj_id,download_path, overwrite):
        pass

    @classmethod
    def load_dag(cls,cid,dag_text):
        pass
        
    @classmethod
    def upload_ipfs_data(cls,TpDestination,decw,download_path,connection_settings):
        pass
    
    @classmethod
    def backup_ipfs_entity(cls,TpSource,item,pinned_cids,download_path,client,overwrite=False):
        pass
        
    @classmethod
    def compare_file_hash(cls,file_path, hash_func='sha2-256'):
        pass
    
    @classmethod
    def has_backedup_cid(cls,download_path,cid):
        pass
     
    @classmethod
    def validate_object(cls,decw,object_id,download_path,connection_settings):
        pass
    
    @classmethod
    def validate_object_attrib(cls,decw,object_id,download_path,connection_settings):
        pass
    
    @classmethod
    def validate_object_payload(cls,decw,object_id,download_path,connection_settings):
        pass
    
    @classmethod
    def overwrite_file_hash(cls,file_path):
        pass

    @classmethod
    def load_entity(cls,filter,download_path):
        pass

    @classmethod
    def upload_object_query(cls,obj_id,download_path,connection_settings,attrib_only = None):
        pass
    
    @classmethod
    def generate_file_hash(cls,file_path):
        pass

    @classmethod
    def push_payload_to(cls,ds_remote,decw,obj,download_path,connection_settings):
        pass
        
    # TODO remove all hard path requirements from this file
    @staticmethod
    def remove_entity(filter:dict,download_path:str):
        pass

    @staticmethod
    def remove_attrib(filter:dict,download_path:str):
        pass

    @staticmethod
    def remove_payload(filter:dict,download_path:str):
        pass

    @staticmethod
    def corrupt_attrib(filter:dict,download_path:str):
        pass

    @staticmethod
    def corrupt_attrib_filename(filter:dict,download_path:str):
        pass
        
    @staticmethod
    def corrupt_payload(filter:dict,download_path:str):
        pass
'''

class DsLocalFilesystem(DsGeneralLocal):
    @classmethod
    def download_object(cls, TpSource, decw, object_ids, download_path, connection_settings, overwrite=False, attrib=None):
        return super().download_object(TpSource, decw, object_ids, download_path, connection_settings, overwrite=overwrite, attrib=attrib)

    @classmethod
    def merge_payload_from_remote(cls, TpRemote, decw, obj, download_path, connection_settings, overwrite):
        return super().merge_payload_from_remote(TpRemote, decw, obj, download_path, connection_settings, overwrite)

    @classmethod
    def merge_attrib_from_remote(cls, TpSource, decw, obj_id, download_path, overwrite):
        return super().merge_attrib_from_remote(TpSource, decw, obj_id, download_path, overwrite)

    @classmethod
    def load_dag(cls, cid, dag_text):
        return super().load_dag(cid, dag_text)

    @classmethod
    def upload_ipfs_data(cls, TpDestination, decw, download_path, connection_settings):
        return super().upload_ipfs_data(TpDestination, decw, download_path, connection_settings)

    @classmethod
    def backup_ipfs_entity(cls, TpSource, item, pinned_cids, download_path, client, overwrite=False):
        return super().backup_ipfs_entity(TpSource, item, pinned_cids, download_path, client, overwrite=overwrite)

    @classmethod
    def compare_file_hash(cls, file_path, hash_func='sha2-256'):
        return super().compare_file_hash(file_path, hash_func=hash_func)

    @classmethod
    def has_backedup_cid(cls, download_path, cid):
        return super().has_backedup_cid(download_path, cid)

    @classmethod
    def validate_object(cls, decw, object_id, download_path, connection_settings):
        return super().validate_object(decw, object_id, download_path, connection_settings)

    @classmethod
    def validate_object_attrib(cls, decw, object_id, download_path, connection_settings):
        return super().validate_object_attrib(decw, object_id, download_path, connection_settings)

    @classmethod
    def validate_object_payload(cls, decw, object_id, download_path, connection_settings):
        return super().validate_object_payload(decw, object_id, download_path, connection_settings)

    @classmethod
    def overwrite_file_hash(cls, file_path):
        return super().overwrite_file_hash(file_path)

    @classmethod
    def load_entity(cls, filter, download_path):
        return super().load_entity(filter, download_path)

    @classmethod
    def upload_object_query(cls, obj_id, download_path, connection_settings, attrib_only=None):
        return super().upload_object_query(obj_id, download_path, connection_settings, attrib_only=attrib_only)

    @classmethod
    def generate_file_hash(cls, file_path):
        return super().generate_file_hash(file_path)

    @classmethod
    def push_payload_to(cls, ds_remote, decw, obj, download_path, connection_settings):
        return super().push_payload_to(ds_remote, decw, obj, download_path, connection_settings)

    # TODO remove all hard path requirements from this file
    @staticmethod
    def remove_entity(filter: dict, download_path: str):
        return DsGeneralLocal.remove_entity(filter, download_path)

    @staticmethod
    def remove_attrib(filter: dict, download_path: str):
        return DsGeneralLocal.remove_attrib(filter, download_path)

    @staticmethod
    def remove_payload(filter: dict, download_path: str):
        return DsGeneralLocal.remove_payload(filter, download_path)
    
    ### Testing Related
    @staticmethod
    def corrupt_attrib(filter: dict, download_path: str):
        return DsGeneralLocal.corrupt_attrib(filter, download_path)

    @staticmethod
    def corrupt_attrib_filename(filter: dict, download_path: str):
        return DsGeneralLocal.corrupt_attrib_filename(filter, download_path)

    @staticmethod
    def corrupt_payload(filter: dict, download_path: str):
        return DsGeneralLocal.corrupt_payload(filter, download_path)
