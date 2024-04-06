import decelium_wallet.core as core
import ipfshttpclient
import os
import json
import pprint
import pandas
from Messages import ObjectMessages

import cid
import multihash
import traceback as tb
import hashlib

class Migrator():
    @staticmethod
    def is_directory(decw,connection_settings,hash):
        # Example pseudocode for IPFS. You'll need to adapt this based on your IPFS client library or command line usage
        _,ipfs_api = decw.net.create_ipfs_connection(connection_settings)
        object_info = ipfs_api.ls(hash)
        object_info = object_info['Objects'][0]
        # print(object_info)
        if object_info and 'Links' in object_info:
            for link in object_info['Links']:
                if len(link['Name']) > 0:
                    return True
            # return len(object_info['Links']) > 0  # Has links, likely a directory
        return False  # No links, likely a file
    
    @staticmethod
    def find_batch_object_ids(decw,offset,limit,filter=None):
        if filter ==None:
            filter ={'attrib':{'file_type':'ipfs'}}
        filter['limit'] = limit
        filter['offset'] = offset
        docs = decw.net.list(filter)
        obj_ids = []
        for doc in docs:
            obj_ids.append(doc['self_id'])
        return obj_ids
    
    @staticmethod
    def find_batch_cids(decw,offset,limit,filter=None):
        found = []
        if filter ==None:
            filter ={'attrib':{'file_type':'ipfs'}}
        filter['limit'] = limit
        filter['offset'] = offset
        docs = decw.net.list(filter)
        returned = len(docs)
        for doc in docs:
            if 'settings' in doc.keys():
                if 'ipfs_cid' in doc['settings'].keys():
                    rec = {"self_id":doc['self_id']}
                    rec['cid'] = doc['settings']['ipfs_cid']
                    found.append(rec)
                if 'ipfs_cids' in doc['settings'].keys():
                    for pin in doc['settings']['ipfs_cids'].values():
                        rec = {"self_id":doc['self_id'],"cid":pin}
                        found.append(rec)                
        return found,returned

    def decelium_has_cids(decw,new_cids):
        all_cids = Migrator.find_all_cids(decw)
        df = pandas.DataFrame(all_cids)
        all_cids = list(df['cid'])
        is_subset = set(new_cids) <= set(all_cids)
        return is_subset

    def ipfs_has_cids(decw,new_cids, connection_settings,refresh=False):
        all_cids = Migrator.ipfs_pin_list(decw, connection_settings,refresh)
        is_subset = set(new_cids) <= set(all_cids)
        #if not is_subset:
        #    print("Missing some CIDS from subset")
        #    print(set(new_cids) - set(all_cids))
        return is_subset

    @staticmethod
    def find_all_cids(decw,offset=0,limit=20): #find all cids on decelium
        found = []
        returned = limit
        while returned >= limit:
            print('.',end="")
            docs,returned = Migrator.find_batch_cids(decw,offset,limit)
            offset = offset + limit
            found = found + docs
        print('')
        return found

    @staticmethod
    def find_download_entity(decw,offset=0,limit=20):
        found = []
        returned = limit
        while returned >= limit:
            print('.',end="")
            docs,returned = Migrator.find_batch_cids(decw,offset,limit)
            offset = offset + limit
            found = found + docs
        return found
    
    def generate_file_hash(file_path):
        hasher = hashlib.sha256()
        
        with open(file_path, 'rb') as f:
            chunk = f.read(4096)  
            while chunk:
                hasher.update(chunk)
                chunk = f.read(4096)
        
        return hasher.hexdigest().encode('utf-8')
    
    def overwrite_file_hash(file_path):
        current_hash = Migrator.generate_file_hash(file_path)
        with open(file_path + ".hash", 'wb') as f:
                f.write(current_hash)        

    def backup_ipfs_entity(item,current_pins,download_path,client,overwrite=False):
        new_cids = []
        assert 'cid' in item
        cid = item['cid']
        file_path = os.path.join(download_path, cid)
        
        # Check if the file already exists to avoid double writing
        for relevant_file in  [file_path+".file",file_path+".dag"]:
            if os.path.exists(relevant_file) and overwrite == False:
                if Migrator.compare_file_hash(relevant_file) == True:
                    return new_cids

        cids = current_pins
        # print("FAILING ON KEYS")
        #print(cids)         
        #if type(current_pins) == dict:
        #    cids = current_pins['Keys']
        try:
            # Check if the item is pinned on this node
            pinned = False
            if cid in cids:
                pinned = True
            if not pinned:
                return new_cids

            # If pinned, proceed to download
            try:
                res = client.cat(cid)
                #with open(file_path+".file", 'wb') as f:
                #    f.write(res)
                with open(file_path + ".file", 'wb') as f:
                    for chunk in client.cat(cid, stream=True):
                        f.write(chunk)

                current_hash = Migrator.generate_file_hash(file_path+ ".file")
                with open(file_path + ".file.hash", 'wb') as f:
                        f.write(current_hash)
                
            except Exception as e:
                if "is a directory" in str(e):
                    dir_json = Migrator.backup_directory_dag(client,cid)
                    for new_item in dir_json['Links']:
                        #print(item)
                        #print(dir_json)
                        new_cids.append({'self_id':item['self_id'],'cid':new_item['Hash']})
                    # dir_json = client.object.get(cid)
                    # print(json.dumps(dict(dir_json)))
                    with open(file_path+".dag", 'w') as f:
                        f.write(json.dumps(dir_json))
                    Migrator.overwrite_file_hash(file_path+ ".dag")
                    print("Finished Directory")
                else:
                    raise e
            return new_cids
        except Exception as e:
            print(f"Error downloading {cid}: {e}")
            print(tb.format_exc())
            return new_cids

    @staticmethod
    def ipfs_pin_list(decw, connection_settings,refresh=False):
        pins = decw.net.download_pin_status({
                'api_key':"UNDEFINED",
                'do_refresh':refresh,
                'connection_settings':connection_settings})       
        assert not 'error' in pins
        return pins
    
    def download_ipfs_data(decw,docs, download_path, connection_settings,overwrite=False):
        c = connection_settings
        # Ensure the download directory exists
        if not os.path.exists(download_path):
            os.makedirs(download_path)
        ipfs_string = f"/dns/{c['host']}/tcp/{c['port']}/{c['protocol']}"

        current_docs = docs
        next_batch = []
        #with ipfshttpclient.connect(ipfs_string) as client:
        #    try:
        #        pins = client.pin.ls(type='recursive')
        #    except Exception as pin_check_error:
        #        print(f"Error checking pin status for {cid}: {pin_check_error}")
        pins = Migrator.ipfs_pin_list(decw, connection_settings)
        with ipfshttpclient.connect(ipfs_string) as client:
            while len(current_docs) > 0:
                for item in current_docs:
                    dic = item
                    if type(item) == str:
                        dic = {'cid':item,'self_id':None}
                    print("BACKING UP CID"+str(item))
                    new_pins = Migrator.backup_ipfs_entity(dic,pins,download_path,client,overwrite)
                    if len(new_pins) > 0:
                        next_batch = next_batch + new_pins
                print("Moving to new batch-------------")
                current_docs = next_batch
                next_batch = []

    @staticmethod
    def upload_ipfs_data(decw,download_path,connection_settings):
        uploaded_something = False
        # for root, dirs, files in os.walk(download_path):
        #    for file in files:
        #    file_path = os.path.join(root, file)
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
            result = decw.net.create_ipfs({
                    'api_key':"UNDEFINED",
                    'file_type':'ipfs', 
                    'connection_settings':connection_settings,
                    'payload_type':payload_type,
                    'payload':file_path})
            messages = ObjectMessages("Migrator.upload_ipfs_data")
            messages.add_assert(result[0]['cid'] in file_path,"Could not local file for "+result[0]['cid'] ) 
            cids.append(result[0]['cid'])
            all_cids = Migrator.ipfs_pin_list(decw, connection_settings,True)            
        return cids,messages
    @staticmethod
    def load_entity(filter,download_path):
        assert 'self_id' in filter
        assert 'attrib' in filter and filter['attrib'] == True
        try:
            with open(download_path+'/'+filter['self_id']+'/object.json','r') as f:
                obj_attrib = json.loads(f.read())
            return obj_attrib
        except:
            return {'error':"Could not read a valid object.json from "+download_path+'/'+filter['self_id']+'/object.json'}
            
    def __merge_attrib_from_remote(decw,obj_id,download_path, overwrite):
        # Load local. If the local has a problem
        # TODO - Validate the object before commiting to a local merge. As if a Decelium miner is broken, one could end up destroying a backup.
        # TODO - Handle merges both ways (could be used by push and pull as an underlying mechanism)
        remote_obj = decw.net.download_entity( {'api_key':'UNDEFINED', 'self_id':obj_id,'attrib':True})
        local_obj = Migrator.load_entity({'api_key':'UNDEFINED', 'self_id':obj_id,'attrib':True},download_path)
        file_path = os.path.join(download_path,obj_id,'object.json')
        # Is the local accurate?
        print("TRYING TO COMPUTE A HASH")
        local_is_valid = Migrator.compare_file_hash(file_path)
        print("FINISHED HASH TO COMPUTE")
        print(local_is_valid)
        if local_is_valid != True:
            local_obj = {'error':'__merge_attrib_from_remote() found that the object is invalid using compare_file_hash() '}


        priority = 'local' if overwrite == False else 'remote'        
        assert 'error'  in remote_obj or 'self_id' in remote_obj
        assert 'error'  in local_obj or 'self_id' in local_obj
        merge_messages = ObjectMessages("Migrator.__merge_attrib_from_remote(for obj_id)"+str(obj_id) )
        if priority == 'local':
            if  'error' in local_obj and 'error' in remote_obj:
                merged_object =  None
                do_write = False
            elif  'self_id' in local_obj and 'error' in remote_obj:
                merged_object = local_obj
                do_write = False
            elif 'error' in local_obj and 'self_id' in remote_obj: 
                merged_object =  remote_obj
                do_write = True
            elif 'self_id' in local_obj and 'self_id' in remote_obj: 
                merged_object = local_obj
                do_write = False

        if priority == 'remote':
            if  'error' in local_obj and 'error' in remote_obj:
                merged_object =  None
                do_write = False
            elif 'self_id' in local_obj and 'error' in remote_obj:
                merged_object = local_obj
                do_write = False
            elif 'error' in local_obj and 'self_id' in remote_obj: 
                merged_object =  remote_obj
                do_write = True
            elif 'self_id' in local_obj and 'self_id' in remote_obj: 
                merged_object = local_obj
                if str(local_obj) != str(remote_obj):
                    merged_object = remote_obj
                    do_write = True

        if merge_messages.add_assert(merged_object != None,"There is no local or remote object to consider during pull" ) == False:
            return False,merged_object,merge_messages

        if do_write == True and merged_object:
            dir_path = os.path.join(download_path, obj_id)
            if not os.path.exists(dir_path):
                os.makedirs(dir_path)
            file_path = os.path.join(dir_path, 'object.json')
            with open(file_path,'w') as f:
                f.write(json.dumps(merged_object)) 

            Migrator.overwrite_file_hash(file_path)
            return True,merged_object,merge_messages
        
        if do_write == False and merged_object:
            return True,merged_object,merge_messages
        raise Exception("Should never reach the end of this function.")
    
    def __merge_payload_from_remote(decw,obj,download_path,connection_settings, overwrite):
        merge_messages = ObjectMessages("Migrator.__merge_payload_from_remote(for obj_id)"+str(obj['self_id']) )

        new_cids = [obj['settings']['ipfs_cid']]
        if 'ipfs_cids' in obj['settings']:
            for cid in obj['settings']['ipfs_cids'].values():
                new_cids.append(cid)
        
        result = Migrator.download_ipfs_data(decw,new_cids, download_path+'/'+obj['self_id'], connection_settings,overwrite)
        return result

    @staticmethod
    def download_object(decw,object_ids,download_path,connection_settings, overwrite=False):
        if type(object_ids) == str:
            object_ids = [object_ids]
        results = {}
        for obj_id in object_ids:
            messages = ObjectMessages("Migrator.download_object(for {obj_id})")
            try:
                success,merged_object,merge_messages = Migrator.__merge_attrib_from_remote(decw,obj_id,download_path, overwrite)
                messages.append(merge_messages)
                print ("Merge success ")
                print(success)
                if success:
                    if messages.add_assert(merged_object['self_id'] == obj_id,"There is a serious consistency problem with the local DB. Halt now" ) == False:
                        raise Exception("Halt Now. The data is corrupt.")
                    print ("Merging payload ")
                    Migrator.__merge_payload_from_remote(decw,merged_object,download_path,connection_settings, overwrite)
                    print ("Finished payload ")
                    results[obj_id] = (True,messages)
                else:
                    results[obj_id] = (False,messages)
            except:
                exc = tb.format_exc()
                with open(download_path+'/'+obj_id+'/object_error.txt','w') as f:
                    f.write(exc)
                messages.add_assert(False,"Exception encountered for "+obj_id+": "+exc ) 
                results[obj_id] = (False,messages)
        return results

    @staticmethod
    def validate_local_against_remote_object(decw,object_id,download_path,connection_settings):
        messages = ObjectMessages("Migrator.validate_local_against_remote_object(for {object_id})")

        # Compares the local object with the remote
        obj_remote = decw.net.download_entity( {'api_key':'UNDEFINED', 'self_id':object_id,'attrib':True})
        with open(download_path+'/'+object_id+'/object.json','r') as f:
            obj_local = json.loads(f.read())

        messages.add_assert(obj_local['self_id'] == obj_remote['self_id'] ,"local.self_id is not identical to remote.self_id "+object_id )
        messages.add_assert(obj_local['parent_id'] == obj_remote['parent_id'] ,"local.parent_id is not identical to remote.parent_id "+object_id )
        messages.add_assert(obj_local['dir_name'] == obj_remote['dir_name'] ,"local.dir_name is not identical to remote.dir_name "+object_id )
        if messages.add_assert('settings' in obj_local  and  'settings' in obj_remote 
                              ,"local or remote do not have settings "+object_id ):
            messages.add_assert(obj_local['settings']['ipfs_cid'] == obj_remote['settings']['ipfs_cid'] 
                               ,"local.ipfs_cid is not identical to remote.ipfs_cid "+object_id )
            messages.add_assert(obj_local['settings']['ipfs_name'] == obj_remote['settings']['ipfs_name'] 
                               ,"local.ipfs_name is not identical to remote.ipfs_name "+object_id )
        
        messages.add_assert(obj_local['settings']['ipfs_cid'] == obj_remote['settings']['ipfs_cid'] ,"local.dir_name is not identical to remote.dir_name "+object_id )
        if 'ipfs_cids' in obj_local['settings']:
            for key in obj_local['settings']['ipfs_cids'].keys():
                messages.add_assert(obj_local['settings']['ipfs_cids'][key] == obj_remote['settings']['ipfs_cids'][key] 
                                   ,"local mismatch in keys for "+object_id )
            for key in obj_remote['settings']['ipfs_cids'].keys():
                messages.add_assert(obj_local['settings']['ipfs_cids'][key] == obj_remote['settings']['ipfs_cids'][key] 
                                   ,"remote mismatch in keys for "+object_id )
                
        for item in os.listdir(download_path+'/'+object_id):
            # Construct the full path of the item-
            file_path = os.path.join(download_path+'/'+object_id, item)
            if file_path.endswith('.file'):
                payload_type = 'local_path'
            elif file_path.endswith('.dag'):
                payload_type = 'ipfs_pin_list'
            else:
                continue
            messages.add_assert(os.path.exists(file_path) == True 
                                ,"The local file does not exist "+object_id )        
                
        return len(messages.get_error_messages() == 0),messages

    @staticmethod
    def validate_local_object(decw,object_id,download_path,connection_settings):
        # Validate the local representation of an object
        messages = ObjectMessages("Migrator.validate_local_object(for {object_id})")
        try:
            file_path_test = download_path+'/'+object_id+'/object.json'
            with open(file_path_test,'r') as f:
                obj_local = json.loads(f.read())
            valido_hasho = Migrator.compare_file_hash(file_path_test)
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
                    valido_hasho = Migrator.compare_file_hash(file_path, hash_func='sha2-256')
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
    
    @staticmethod
    def validate_remote_object(decw,object_id,download_path,connection_settings,obj_remote = None):
        # Compares the local object with the remote
        messages = ObjectMessages("Migrator.validate_remote_object(for {"+object_id+"})")
        if obj_remote == None:
            obj_remote = decw.net.download_entity( {'api_key':'UNDEFINED', 'self_id':object_id,'attrib':True})
        '''
        assert obj_remote['self_id'] == obj_remote['self_id'] # CHANGEASSERT
        assert obj_remote['parent_id'] == obj_remote['parent_id'] # CHANGEASSERT
        assert obj_remote['dir_name'] == obj_remote['dir_name'] # CHANGEASSERT
        assert obj_remote['settings']['ipfs_cid'] == obj_remote['settings']['ipfs_cid'] # CHANGEASSERT
        cids = [obj_remote['settings']['ipfs_cid']]
        assert obj_remote['settings']['ipfs_name'] == obj_remote['settings']['ipfs_name'] # CHANGEASSERT
        if 'ipfs_cids' in obj_remote['settings']:
            for key in obj_remote['settings']['ipfs_cids'].keys():
                assert obj_remote['settings']['ipfs_cids'][key] # CHANGEASSERT
            cids.append(obj_remote['settings']['ipfs_cids'][key])
        '''
        cids_pinned = []
        for k in ['self_id','parent_id','dir_name','settings']:
            if messages.add_assert(k in obj_remote and obj_remote[k] != None, "missing {k} for {object_id}") == False:
                return False, messages
        
        if messages.add_assert('ipfs_cid' in obj_remote['settings'], "missing settings.ipfs_cid for {object_id}"):
            cids_pinned.append (obj_remote['settings']['ipfs_cid'] )

        messages.add_assert('ipfs_name' in obj_remote['settings'], "missing settings.ipfs_name for {object_id}")
        if 'ipfs_cids' in obj_remote['settings']:
            for key in obj_remote['settings']['ipfs_cids'].keys():
                if messages.add_assert(key in obj_remote['settings']['ipfs_cids'], "missing {key} from settings.ipfs_cids for {object_id}"):
                    cids_pinned.append (obj_remote['settings']['ipfs_cids'][key] )
        for cid in cids_pinned:
            result = decw.net.check_pin_status({
                    'api_key':"UNDEFINED",
                    'connection_settings':connection_settings,
                    'cid': cid})
            messages.add_assert(result == True, "cid is missing from remote "+cid)
        print("Migrator Finished")


        return len(messages.get_error_messages()) == 0, messages

    @staticmethod
    def upload_object_query(decw,obj_id,download_path,connection_settings):
        '''
            Validates the object, and generates a query to reupload the exact object
        '''
        messages = ObjectMessages("Migrator.upload_object_query(for {"+obj_id+"})")
        if messages.add_assert(os.path.isfile(download_path+'/'+obj_id+'/object.json'), obj_id+"is missing an object.json") == False:
            return False,messages
            
        with open(download_path+'/'+obj_id+'/object.json','r') as f:
            obj = json.loads(f.read()) 

        if messages.add_assert('settings' in obj, "no settings in "+obj_id) == False:
            return False,messages
        if messages.add_assert('ipfs_cid' in obj['settings'], "ipfs_cid is missing from local object. It is invalid. "+obj_id) == False:
            return False,messages
        if messages.add_assert('ipfs_cids' in obj['settings'], "ipfs_cids is missing from local object. It is invalid. "+obj_id) == False:
            return False,messages
        
        #obj_cids = [obj['settings']['ipfs_cid']]
        obj_cids = []
        for path,cid in obj['settings']['ipfs_cids'].items():
            cid_record = { 'cid':cid,
                           'name':path }
            cid_record['name'] = cid_record['name'].replace(obj_id+'/',"")
            if len(path) > 0:
                cid_record['root'] = True
            obj_cids.append(cid_record)
            #print(cid_record)
            file_exists = os.path.isfile(download_path+'/'+obj_id+'/'+cid+'.file') or os.path.isfile(download_path+'/'+obj_id+'/'+cid+'.dag')      
            if messages.add_assert(file_exists == True, "Could not fild the local file for "+obj_id+":"+cid) == False:
                return False,messages

        query = {
            'attrib':obj
        }
        return query,messages

    @staticmethod
    def backup_directory_dag(client, cid, path=""):
        item_details_response = client.object.get(cid)
        item_details = {
            'Links': [{
                'Name': link['Name'],
                'Hash': link['Hash'],
                'Size': link['Size']
            } for link in item_details_response['Links']]
        }

        return item_details
        
    @staticmethod
    def compare_file_hash(file_path, hash_func='sha2-256'):
        if not os.path.exists(file_path):
            return None
        current_hash = Migrator.generate_file_hash(file_path)
        if not os.path.exists(file_path+".hash"):
            return None
        with open(file_path + ".hash", 'rb') as f:
                stored_hash = f.read()
        if stored_hash == current_hash:
            return True
        return False


