import hashlib
import os
import json
import datetime
import random
import shutil

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

class UtilFile:
    @classmethod
    def remove_payload(cls,filter:dict,download_path:str):
        assert 'self_id' in filter
        object_id = filter['self_id']
        obj_backup_path = os.path.join(download_path,object_id)
        if not os.path.exists(obj_backup_path):
            True
        for item in os.listdir(obj_backup_path):
            # Construct the full path of the item-
            file_path = os.path.join(obj_backup_path, item)
            if item.endswith('.file') or item.endswith('.dag'):
                os.remove(file_path)
                if os.path.exists(file_path):
                    return {'error':'could not remove item '+file_path}
        return True    

    @classmethod
    def check_hash(cls,obj_id,download_path):
        file_path = os.path.join(download_path,obj_id,'object.json')
        local_is_valid = cls.compare_file_hash(file_path)
        return local_is_valid
    
    @classmethod
    def overwrite_file_hash(cls,file_path):
        current_hash = cls.generate_file_hash(file_path)
        with open(file_path + ".hash", 'wb') as f:
                f.write(current_hash)  

    @classmethod
    def load_object_file(cls,download_path,object_id):
        file_path_test = download_path+'/'+object_id+'/object.json'
        with open(file_path_test,'r') as f:
            obj_local = json.loads(f.read())
        return obj_local
    
    @classmethod
    def validate_payload_files(cls,download_path,object_id):
        invalid_list = []
        cids_downloaded = []
        for item in os.listdir(download_path+'/'+object_id):
            file_path = os.path.join(download_path+'/'+object_id, item)            
            if item.endswith('.file') or item.endswith('.dag'):
                try:
                    valido_hasho = cls.compare_file_hash(file_path, hash_func='sha2-256')
                    if valido_hasho != True:
                        invalid_list.append({'cid':item.split('.')[0],"message":"Encountered A bad hash for cid:"+file_path})

                except:
                    import traceback as tb  
                    invalid_list.append({'cid':item.split('.')[0],"message":"Encountered an exception with the internal hash validation:"+tb.format_exc()})
            
            if item.endswith('.file') or item.endswith('.dag'):
                cids_downloaded.append(item.split('.')[0])
        return cids_downloaded, invalid_list

    @classmethod
    def validate_object_file(cls,download_path,object_id):
        file_path_test = download_path+'/'+object_id+'/object.json'
        valido_hasho = cls.compare_file_hash(file_path_test)    
        return  valido_hasho   
    
    @classmethod
    def write_object(cls,obj,download_path):
        obj_id = obj['self_id']
        dir_path = os.path.join(download_path, obj_id)
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
        file_path = os.path.join(dir_path, 'object.json')
        with open(file_path,'w') as f:
            f.write(jsondateencode_local.dumps(obj)) 
        cls.overwrite_file_hash(file_path)
    
    @classmethod
    def write_payload_from_bytes(cls,obj,download_path,the_bytes, overwrite):
        obj_id = obj['self_id']
        ## TODO -- Add result and error if result fails
        if not os.path.exists(download_path+'/'+obj_id):
            os.makedirs(download_path+'/'+obj_id)
        file_path = os.path.join(download_path,obj_id,"payload.file")
        with open(file_path, 'wb') as f:
            f.write(the_bytes)

        current_hash = cls.generate_file_hash(file_path)
        with open(file_path + ".hash", 'wb') as f:
                f.write(current_hash)

    @classmethod
    def remove_attrib(cls,filter:dict,download_path:str):
        assert 'self_id' in filter
        file_path = os.path.join(download_path,filter['self_id'],"object.json")
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
            if os.path.exists(file_path):
                return {'error':'could not remove item'}
            return True
        except:
            import traceback as tb
            return {'error':"Could not remove "+download_path+'/'+filter['self_id'],'traceback':tb.format_exc()}
        
    @classmethod
    def generate_file_hash(cls,file_path):
        # TODO Rename to generate_hash
        hasher = hashlib.sha256()
        
        with open(file_path, 'rb') as f:
            chunk = f.read(4096)  
            while chunk:
                hasher.update(chunk)
                chunk = f.read(4096)
        return hasher.hexdigest().encode('utf-8')    

    @classmethod
    def write_dagfile(cls,download_path,object_name,data):
        file_path = os.path.join(download_path,object_name)
        with open(file_path+".dag", 'w') as f:
            f.write(jsondateencode_local.dumps(data))
        UtilFile.overwrite_file_hash(file_path+ ".dag")
        
    @classmethod
    def has_object(cls,download_path,obj_id):
        return os.path.isfile(download_path+'/'+obj_id+'/object.json')
    
    # TODO - Move Me
    @classmethod
    def has_backedup_cid(cls,download_path,cid):
        file_path = os.path.join(download_path, cid)        
        for relevant_file in  [file_path+".file",file_path+".dag"]:
            if os.path.exists(relevant_file):
                if UtilFile.compare_file_hash(relevant_file) == True:
                    return True
        return False

    @classmethod
    def generwrite_file_hash(cls,download_path,object_name):
        file_path = os.path.join(download_path,object_name)
        current_hash = cls.generate_file_hash(file_path+ ".file")
        with open(file_path + ".file.hash", 'wb') as f:
                f.write(current_hash)

    @classmethod
    def compare_file_hash(cls,file_path, hash_func='sha2-256'):
        if not os.path.exists(file_path):
            return None
        current_hash = cls.generate_file_hash(file_path)
        if not os.path.exists(file_path+".hash"):
            return None
        with open(file_path + ".hash", 'rb') as f:
                stored_hash = f.read()
        if stored_hash == current_hash:
            return True
        return False
    @classmethod
    def init_dir(cls,download_path):
        if not os.path.exists(download_path):
            os.makedirs(download_path)        

    @classmethod
    def get_file_stream(cls,download_path,base_file_name):
        file_path = os.path.join(download_path, base_file_name)
        return open(file_path + ".file", 'wb') 

    @classmethod
    def file_backup_exists(cls,download_path,root_cid):
        raise ("Might not be needed")
        file_path = os.path.join(download_path, root_cid)
        if os.path.exists(file_path + ".file") and os.path.exists(file_path + ".file.hash"):
            # print("backup_ipfs_entity cached ")
            return True
        return False
    @classmethod
    def compare_object_hash(cls,download_path,obj_id, hash_func='sha2-256'):
        cls.init_dir()      
        file_path = os.path.join(download_path, obj_id+ '.file')
        return cls.compare_file_hash(file_path,hash_func)

    @classmethod
    def load_dag(cls,cid,dag_text):
        ''' Parsers
        {"Links": [{"Name": ".DS_Store", "Hash": "QmVKugVyynbLDmwgxHm9Z6JZMjqtyVNH6MqgxTxhTXX2US", "Size": 6159},
                    {"Name": "img_test.png", "Hash": "QmYZsomCw9J9Fb8hLgiB7iA3W1iTYnLi7hbJXq3Bggz2rL", "Size": 460641}, 
                    {"Name": "test.txt", "Hash": "QmQkBHa6uAcVm8bwfoufcmAiG25vfNYdo3Lvrt9Q7QWmZR", "Size": 25}, 
                    {"Name": "test_sub", "Hash": "QmbGSb2Gerf3WQUeS78yvEcYcSkTvurQghktXT3y9Fao6S", "Size": 77}]}  
        '''
        dag_json = json.loads(dag_text)
        assert "Links" in dag_json, "Dont have a links field " + str(dag_json)
        cid_list = dag_json["Links"]
        children = []
        for child in cid_list: 
            assert "Name" in child, "No Name " + str(dag_json)
            assert "Hash" in child, "No Hash " + str(dag_json)
            children.append(child["Hash"])
        return children

    @classmethod
    def build_upload_sequence(cls,download_path):
        tree = CidTree()
        for item in os.listdir(download_path):
            file_path = os.path.join(download_path, item)
            if not file_path.endswith(".dag"):
                continue
            dag_text = ""
            cid = item.replace(".dag","")
            with open(file_path,'r') as f:
                dag_text = f.read()
            dag_list = cls.load_dag(cid,dag_text)
            for child_cid in dag_list:
                tree.add_dependency(cid, child_cid)
        for cid in tree.get_upload_sequence(): 
            assert os.path.join(download_path, cid+".file") or os.path.join(download_path, cid+".hash")
        
        return tree.get_upload_sequence()
    
    @classmethod
    def do_upload_by_type(cls,TpDestination,decw,type_str,download_path,messages,connection_settings):
        uploaded_cids = []
        for item in os.listdir(download_path):
            file_path = os.path.join(download_path, item)
            if not file_path.endswith(type_str):
                continue
            print("UPLOADING",file_path)
            if file_path.endswith('.file'):
                payload_type = 'local_path'
            elif file_path.endswith('.dag'):
                payload_type = 'ipfs_pin_list'
            else:
                continue
            result = TpDestination.upload_path_to_ipfs(decw,connection_settings,payload_type,file_path)
            try:
                result[0]
                messages.add_assert(result[0]['cid'] in file_path,"A. Could not locate file for "+result[0]['cid'] ) 
                uploaded_cids.append(result[0]['cid'])
            except:
                messages.add_assert(False,"A. could not parse upload_ipfs_data() result: "+str(result) ) 
        return uploaded_cids


    @classmethod
    def assert_dag_exists(cls,download_path,cid):
        file_path = os.path.join(download_path, cid+".dag") ####
        assert os.path.exists(file_path), "Internal impossible situation. DAG missing "+file_path ###

    @classmethod
    def get_dag_path(cls,download_path,cid):
        return os.path.join(download_path, cid+".dag") 
    


    @staticmethod
    def corrupt_attrib(filter:dict,download_path:str):
        assert 'self_id' in filter
        self_id = filter['self_id']
        file_path = os.path.join(download_path, self_id, 'object.json')
        random_bytes_size = 1024
        random_bytes = random.getrandbits(8 * random_bytes_size).to_bytes(random_bytes_size, 'little')
        with open(file_path, 'wb') as corrupt_file:
            corrupt_file.write(random_bytes)
        return True

    @staticmethod
    def corrupt_attrib_filename(filter:dict,download_path:str):
        assert 'self_id' in filter
        self_id = filter['self_id']
        file_path = os.path.join(download_path, self_id, 'object.json')

        with open(file_path, 'r') as f:
            correct_json = jsondateencode_local.loads(f.read())
        correct_json['dir_name'] = "corrupt_name"
        with open(file_path, 'w') as f:
            f.write(json.dumps(correct_json))
        return True
        
    @staticmethod
    def corrupt_payload(filter:dict,download_path:str):
        assert 'self_id' in filter
        self_id = filter['self_id']
        object_path = os.path.join(download_path, self_id)
        files_affected = []
        for filename in os.listdir(object_path):
            if  filename.endswith('.file'): # filename.endswith('.dag') or
                file_path = os.path.join(object_path, filename)
                random_bytes_size = 1024
                random_bytes = random.getrandbits(8 * random_bytes_size).to_bytes(random_bytes_size, 'little')
                with open(file_path, 'wb') as corrupt_file:
                    corrupt_file.write(random_bytes)
                files_affected.append(file_path)
        return True,files_affected

    @classmethod
    def dump_object_log(cls,filename,download_path,obj_id,data):
        if not os.path.exists(download_path+'/'+obj_id):
            os.makedirs(download_path+'/'+obj_id)
        with open(download_path+'/'+obj_id+'/'+filename,'w') as f:
            f.write(data)
        return True

    @staticmethod
    def remove_entity(filter:dict,download_path:str):
        assert 'self_id' in filter
        file_path = os.path.join(download_path,filter['self_id'])
        try:
            if os.path.exists(file_path):
                shutil.rmtree(file_path)
            if os.path.exists(file_path):
                return {'error':'could not remove item'}
            return True
        except:
            import traceback as tb
            return {'error':"Could not remove "+download_path+'/'+filter['self_id'],'traceback':tb.format_exc()}
        
    @classmethod
    def load_entity(cls,filter,download_path):
        assert 'self_id' in filter
        assert 'attrib' in filter and filter['attrib'] == True
        try:
            with open(download_path+'/'+filter['self_id']+'/object.json','r') as f:
                obj_attrib = jsondateencode_local.loads(f.read())
            return obj_attrib
        except:
            return {'error':"Could not read a valid object.json from "+download_path+'/'+filter['self_id']+'/object.json'}