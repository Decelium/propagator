import hashlib
import os
import json
import datetime

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
            print("UtilFile - FINAL INVALID LIST: Returning")
            print(cids_downloaded)
            print(invalid_list)
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
        hasher = hashlib.sha256()
        
        with open(file_path, 'rb') as f:
            chunk = f.read(4096)  
            while chunk:
                hasher.update(chunk)
                chunk = f.read(4096)
        return hasher.hexdigest().encode('utf-8')    

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