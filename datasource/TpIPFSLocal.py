import decelium_wallet.core as core
import os
import json
from Messages import ObjectMessages
import traceback as tb
import hashlib

class TpIPFSLocal():
    @classmethod
    def upload_ipfs_data(cls,TpDestination,decw,download_path,connection_settings):
        cids = [] 
        for item in os.listdir(download_path):
            # Construct the full path of the item-
            file_path = os.path.join(download_path, item)
            if file_path.endswith('.file'):
                payload_type = 'local_path'
            elif file_path.endswith('.dag'):
                payload_type = 'ipfs_pin_list'
            else:
                continue
            result = TpDestination.upload_path_to_ipfs(decw,connection_settings,payload_type,file_path)
            messages = ObjectMessages("Migrator.upload_ipfs_data")
            messages.add_assert(result[0]['cid'] in file_path,"Could not local file for "+result[0]['cid'] ) 
            cids.append(result[0]['cid'])
            # all_cids = TpIPFSDecelium.ipfs_pin_list(decw, connection_settings,True)            
        return cids,messages
    
    @classmethod
    def backup_ipfs_entity(cls,TpSource,item,pinned_cids,download_path,client,overwrite=False):
        new_cids = []
        assert 'cid' in item
        root_cid = item['cid']
        if not os.path.exists(download_path):
            os.makedirs(download_path)

        file_path = os.path.join(download_path, root_cid)
        # Check if root the file already exists to avoid double writing
        if overwrite == False and TpIPFSLocal.has_backedup_cid(download_path, root_cid) == True:
            return new_cids
        try:
            # Check if the item is pinned on this node
            pinned = False
            if root_cid in pinned_cids:
                pinned = True
            if not pinned:
                return new_cids
            #TpIPFSDecelium.ipfs_has_cids(decw,)
            # If pinned, proceed to download
            try:
                res = client.cat(root_cid)
                #with open(file_path+".file", 'wb') as f:
                #    f.write(res)
                with open(file_path + ".file", 'wb') as f:
                    for chunk in TpSource.get_cid_read_stream(client,root_cid):
                        f.write(chunk)

                current_hash = cls.generate_file_hash(file_path+ ".file")
                with open(file_path + ".file.hash", 'wb') as f:
                        f.write(current_hash)
                
            except Exception as e:
                if "is a directory" in str(e):
                    dir_json = TpSource.download_directory_dag(client,root_cid)
                    for new_item in dir_json['Links']:
                        new_cids.append({'self_id':item['self_id'],'cid':new_item['Hash']})
                    # dir_json = client.object.get(cid)
                    # print(json.dumps(dict(dir_json)))
                    with open(file_path+".dag", 'w') as f:
                        f.write(json.dumps(dir_json))
                    TpIPFSLocal.overwrite_file_hash(file_path+ ".dag")
                else:
                    raise e
            return new_cids
        except Exception as e:
            print(f"Error downloading {cid}: {e}")
            print(tb.format_exc())
            return new_cids    
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
    def has_backedup_cid(cls,download_path,cid):
        file_path = os.path.join(download_path, cid)        
        for relevant_file in  [file_path+".file",file_path+".dag"]:
            if os.path.exists(relevant_file):
                if cls.compare_file_hash(relevant_file) == True:
                    return True
        return False
     

    @classmethod
    def validate_local_object(cls,decw,object_id,download_path,connection_settings):
        # Validate the local representation of an object
        messages = ObjectMessages("TpIPFSLocal.validate_local_object(for {object_id})")
        try:
            file_path_test = download_path+'/'+object_id+'/object.json'
            with open(file_path_test,'r') as f:
                obj_local = json.loads(f.read())
            valido_hasho = cls.compare_file_hash(file_path_test)
            if valido_hasho != True:
                False,messages.add_assert(False, "Encountered A bad hash object.json :"+file_path_test)

        except:
            messages.add_assert(False==True, "Could not validate presense of file file")
            return False,messages

        cids_pinned = []
        cids_downloaded = []

        for k in ['self_id','parent_id','dir_name','settings']:
            messages.add_assert(k in obj_local and obj_local[k] != None, "missing {k} for {object_id}")
        if messages.add_assert('ipfs_cid' in obj_local['settings'], "missing settings.ipfs_cid for {object_id}"):
            cids_pinned.append (obj_local['settings']['ipfs_cid'] )

        messages.add_assert('ipfs_name' in obj_local['settings'], "missing settings.ipfs_name for {object_id}")
        if 'ipfs_cids' in obj_local['settings']:
            for key in obj_local['settings']['ipfs_cids'].keys():
                if messages.add_assert(key in obj_local['settings']['ipfs_cids'], "missing {key} from settings.ipfs_cids for {object_id}"):
                    cids_pinned.append (obj_local['settings']['ipfs_cids'][key] )
        invalid_list = []
        for item in os.listdir(download_path+'/'+object_id):
            # Construct the full path of the item-
            file_path = os.path.join(download_path+'/'+object_id, item)
            
            if item.endswith('.file') or item.endswith('.dag'):
                try:
                    valido_hasho = cls.compare_file_hash(file_path, hash_func='sha2-256')
                    if valido_hasho != True:
                        invalid_list.append(item.split('.')[0])
                        messages.add_assert(False, "Encountered A bad hash for cid:"+file_path)

                except:
                    messages.add_assert(False, "Encountered an exception with the internal hash validation:"+tb.format_exc())
            
            if item.endswith('.file') or item.endswith('.dag'):
                cids_downloaded.append(item.split('.')[0])
        missing = []
        for pin in cids_pinned:
            messages.add_assert(pin in cids_downloaded, "a local pin from pinned object for "+object_id+" was not downloaded")
        return len(messages.get_error_messages())== 0,messages    

    @classmethod
    def overwrite_file_hash(cls,file_path):
        current_hash = cls.generate_file_hash(file_path)
        with open(file_path + ".hash", 'wb') as f:
                f.write(current_hash)      

    @classmethod
    def load_entity(cls,filter,download_path):
        assert 'self_id' in filter
        assert 'attrib' in filter and filter['attrib'] == True
        try:
            with open(download_path+'/'+filter['self_id']+'/object.json','r') as f:
                obj_attrib = json.loads(f.read())
            return obj_attrib
        except:
            return {'error':"Could not read a valid object.json from "+download_path+'/'+filter['self_id']+'/object.json'}

    @classmethod
    def generate_file_hash(cls,file_path):
        hasher = hashlib.sha256()
        
        with open(file_path, 'rb') as f:
            chunk = f.read(4096)  
            while chunk:
                hasher.update(chunk)
                chunk = f.read(4096)
        
        return hasher.hexdigest().encode('utf-8')