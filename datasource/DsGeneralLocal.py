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
import datetime
from .UtilFile import UtilFile, jsondateencode_local


class DsGeneralLocal(DsGeneral):
    @classmethod
    def download_object(cls,TpSource,decw,object_ids,download_path,connection_settings, overwrite=False,attrib=None):
        if type(object_ids) == str:
            object_ids = [object_ids]
        results = {}
        for obj_id in object_ids:
            messages = ObjectMessages(f"{str(cls)}:in DsGeneralLocal.download_object(for {obj_id})")
            print("DsGeneralLocal download_object/INSIDE DOWNLOAD "+download_path)
            try:
                success,merged_object,merge_messages = cls.merge_attrib_from_remote(TpSource,decw,obj_id,download_path, overwrite)
                messages.append(merge_messages)
                if success:
                    if messages.add_assert(merged_object['self_id'] == obj_id,"There is a serious consistency problem with the local DB. Halt now" ) == False:
                        raise Exception("Halt Now. The data is corrupt.")
                    # TODO should verify the success of the merge operation
                    if attrib != True:
                        result = cls.merge_payload_from_remote(TpSource,decw,merged_object,download_path,connection_settings, overwrite)
                        ################# -----X ################ -----X ################ -----X ################ -----X
                        #results[obj_id] = (True,messages) ################ -----X ################ -----X ################ -----X ################ -----X
                        #continue
                        if type(result) == dict:
                            if messages.add_assert( "error" not in result,"Could not retrieve IPFS pins from merge_payload_from_remote: "+result['error'] ) == False:
                                results[obj_id] = (False,messages)
                        else:
                            results[obj_id] = (result,messages)
                    else:
                        results[obj_id] = (True,messages)
                else:
                    results[obj_id] = (False,messages)
                print(" DsGeneralLocal download_object/FINISHED  DOWNLOAD")
                
            except:
                exc = tb.format_exc()
                if not os.path.exists(download_path+'/'+obj_id):
                    os.makedirs(download_path+'/'+obj_id)
               
                with open(download_path+'/'+obj_id+'/object_error.txt','w') as f:
                    f.write(exc)
                messages.add_assert(False,"Exception encountered for "+obj_id+": "+exc ) 
                results[obj_id] = (False,messages)
        return results
    


    @classmethod
    def merge_payload_from_remote(cls,TpRemote,decw,obj,download_path,connection_settings, overwrite):
        merge_messages = ObjectMessages("TpFile.Local.__merge_payload_from_remote(for obj_id)"+str(obj['self_id']) )
        result,the_bytes = TpRemote.download_payload_data(decw,obj)
        assert result == True, "result is not true: "+str(result)
        assert the_bytes != None, "Could not download payload data"
        UtilFile.write_payload_from_bytes(obj,download_path,the_bytes, overwrite)
        
        return result    

    
    @classmethod        
    def merge_attrib_from_remote(cls,TpSource,decw,obj_id,download_path, overwrite):
        merge_messages = ObjectMessages("Migrator.__merge_attrib_from_remote(for obj_id)"+str(obj_id) )
        #### TODO refac file ops out
        local_obj = cls.load_entity({'api_key':'UNDEFINED', 'self_id':obj_id,'attrib':True},download_path)
        remote_obj = TpSource.load_entity({'api_key':'UNDEFINED', 'self_id':obj_id,'attrib':True},decw)
        
        #### TODO refac file ops out
        #file_path = os.path.join(download_path,obj_id,'object.json')
        ## Is the local accurate?
        #local_is_valid = cls.compare_file_hash(file_path)
        local_is_valid = UtilFile.check_hash(obj_id,download_path)
        if local_is_valid != True:
            local_obj = {'error':'__merge_attrib_from_remote() found that the object is invalid using compare_file_hash() '}

        priority = 'local' if overwrite == False else 'remote'        
        assert 'error'  in remote_obj or 'self_id' in remote_obj
        assert 'error'  in local_obj or 'self_id' in local_obj
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
            else:
                raise Exception("INTERNAL ERROR 2387")

        elif priority == 'remote':
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
                do_write = False
                if str(local_obj) != str(remote_obj):
                    merged_object = remote_obj
                    do_write = True
            else:
                raise Exception("INTERNAL ERROR 5668")
        else:
            raise Exception("INTERNAL ERROR 708")
        if merge_messages.add_assert(merged_object != None,"There is no local or remote object to consider during pull" ) == False:
            return False,merged_object,merge_messages

        if do_write == True and merged_object:
            UtilFile.write_object(merged_object,download_path)
            return True,merged_object,merge_messages
        
        if do_write == False and merged_object:
            return True,merged_object,merge_messages
        raise Exception("Should never reach the end of this function.")
    

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
    def upload_ipfs_data(cls,TpDestination,decw,download_path,connection_settings):
        #raise Exception("I AM RUNNIN")
        cids = [] 
        messages = ObjectMessages("TpGeneralLocal.upload_ipfs_data")
        
        # Sequence and do IPFS upload
        cid_order = UtilFile.build_upload_sequence(download_path)
        uploaded_ids = UtilFile.do_upload_by_type(TpDestination,decw,'.file',download_path,messages,connection_settings)
        cids = cids + uploaded_ids
        
        #
        # Register the upload with 
        for cid in cid_order:
            if cid in uploaded_ids:
                continue
            UtilFile.assert_dag_exists(download_path,cid)
            file_path = UtilFile.get_dag_path(download_path,cid)
            result = TpDestination.upload_path_to_ipfs(decw,connection_settings,'ipfs_pin_list',file_path)
            try:
                result[0]
                messages.add_assert(result[0]['cid'] in file_path,"B. Could not locate file for "+result[0]['cid'] ) 
                cids.append(result[0]['cid'])
            except:
                messages.add_assert(False,"B. could not parse upload_ipfs_data() result: "+str(result) ) 
        
        return cids,messages
    
    @classmethod
    def backup_ipfs_entity(cls,TpSource,item,pinned_cids,download_path,client,overwrite=False):
        new_cids = []
        assert 'cid' in item
        root_cid = item['cid']
        if not os.path.exists(download_path):
            os.makedirs(download_path)
        file_path = os.path.join(download_path, root_cid)
        if os.path.exists(file_path + ".file") and os.path.exists(file_path + ".file.hash"):
            if UtilFile.compare_file_hash(file_path + ".file") == True:
                # print("backup_ipfs_entity cached ")
                return [root_cid]

        # Check if root the file already exists to avoid double writing
        if overwrite == False and cls.has_backedup_cid(download_path, root_cid) == True:
            return new_cids
        try:
            # Check if the item is pinned on this node
            pinned = False
            if root_cid in pinned_cids:
                pinned = True
            if not pinned:
                return new_cids
            
            try:
                res = client.cat(root_cid)
                with open(file_path + ".file", 'wb') as f:
                    for chunk in TpSource.get_cid_read_stream(client,root_cid):
                        f.write(chunk)

                current_hash = UtilFile.generate_file_hash(file_path+ ".file")
                with open(file_path + ".file.hash", 'wb') as f:
                        f.write(current_hash)
                print("backup_ipfs_entity.SHOULD HAVE WRITTEN "+file_path)
                new_cids.append(root_cid)
            except Exception as e:
                
                if "is a directory" in str(e):
                    dir_json = TpSource.download_directory_dag(client,root_cid)
                    for new_item in dir_json['Links']:
                        new_cids.append({'self_id':item['self_id'],'cid':new_item['Hash']})
                    # dir_json = client.object.get(cid)
                    # print(json.dumps(dict(dir_json)))
                    with open(file_path+".dag", 'w') as f:
                        f.write(jsondateencode_local.dumps(dir_json))
                    UtilFile.overwrite_file_hash(file_path+ ".dag")
                else:
                    print("backup_ipfs_entity.failed")
                    raise e
            return new_cids
        except Exception as e:
            print(f"Error downloading : {e}")
            print(tb.format_exc())
            return new_cids    
        

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
    def validate_object(cls,decw,object_id,download_path,connection_settings):
        entity_success,entity_messages = cls.validate_object_attrib(decw,object_id,download_path,connection_settings)
        payload_success,payload_messages = cls.validate_object_payload(decw,object_id,download_path,connection_settings)
        entity_messages:ObjectMessages = entity_messages
        all_messages:ObjectMessages = payload_messages
        all_messages.append(entity_messages)
        return entity_success and payload_success,all_messages
    
    @classmethod
    def validate_object_attrib(cls,decw,object_id,download_path,connection_settings):
        # Validate the local representation of an object
        messages = ObjectMessages("DsGeneralLocal.validate_object_attrib(for {object_id})")
        try:
            valido_hasho = UtilFile.validate_object_file(download_path,object_id)
            if valido_hasho != True:
                messages.add_assert(False, "Encountered A bad hash object.json :"+download_path + object_id)
                return False,messages
        except:
            messages.add_assert(False==True, "Could not validate presense of file file:"+str(download_path+'/'+object_id+'/object.json'))
            return False,messages
        return len(messages.get_error_messages())== 0,messages   
    
    
    @classmethod
    def validate_object_payload(cls,decw,object_id,download_path,connection_settings):
        #raise Exception("I WAS TESTED")
        # Load the entity, and make sure payload is present that matches
        messages = ObjectMessages("DsGeneralLocal.validate_object_payload(for {object_id})")
        try:
            valido_hasho = UtilFile.validate_object_file(download_path,object_id)
            if valido_hasho != True:
                messages.add_assert(False, "B. Encountered A bad hash object.json :"++download_path + object_id)
                return False, messages
        except:
            messages.add_assert(False==True, "B. Could not validate presense of entity file")
            return False,messages
            # Bruh. What t.f. was I talking about. I am afraid to delete this (below): TODO decode this mystery
            #Cha We should make a best effort to validate in the case the object def is missing.
            #return cls.validate_object_payload_only(decw,object_id,download_path,connection_settings)
        
        cids_pinned = []
        cids_downloaded = []
        obj_local = UtilFile.load_object_file(download_path,object_id)
        for k in ['self_id','parent_id','dir_name','settings']:
            messages.add_assert(k in obj_local and obj_local[k] != None, "missing {k} for {object_id}")
        if messages.add_assert('ipfs_cid' in obj_local['settings'], "missing settings.ipfs_cid for {object_id}"):
            cids_pinned.append (obj_local['settings']['ipfs_cid'] )

        messages.add_assert('ipfs_name' in obj_local['settings'], "missing settings.ipfs_name for {object_id}")
        if 'ipfs_cids' in obj_local['settings']:
            for key in obj_local['settings']['ipfs_cids'].keys():
                if messages.add_assert(key in obj_local['settings']['ipfs_cids'], "missing {key} from settings.ipfs_cids for {object_id}"):
                    cids_pinned.append (obj_local['settings']['ipfs_cids'][key] )
        cids_downloaded, invalid_list   = UtilFile.validate_payload_files(download_path,object_id)
        
        for item in invalid_list:
            print("DsGeneralLocal - Printing invalid item")
            print(invalid_list)
            print(item)
            messages.add_assert(False, item['message'])

        missing = []
        for pin in cids_pinned:
            if messages.add_assert(pin in cids_downloaded, "At least one pin ("+pin+") for "+object_id+" is missing") == False:
                break
        return len(messages.get_error_messages())== 0,messages   
    
    

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

    @classmethod
    def upload_object_query(cls,obj_id,download_path,connection_settings,attrib_only = None):
        '''
            Validates the object, and generates a query to reupload the exact object
        '''
        messages = ObjectMessages("DsGeneralLocal.upload_object_query(for {"+obj_id+"})")
        if messages.add_assert(os.path.isfile(download_path+'/'+obj_id+'/object.json'), obj_id+"is missing an object.json") == False:
            return False,messages
        if attrib_only == True:
            decw = None
            local_result, local_validation_messages = cls.validate_object_attrib(decw,obj_id, download_path, connection_settings)
        else:
            decw = None
            local_result, local_validation_messages = cls.validate_object(decw,obj_id, download_path, connection_settings)
        if messages.add_assert(local_result == True, "Did not prepare a query, bcause the data is invalid for "+obj_id) == False:
            messages.append(local_validation_messages)
            return False,messages
        obj = cls.load_entity({'self_id':obj_id,'attrib':True},download_path)
        
        '''
        obj = cls.load_entity({'self_id':obj_id,'attrib':True},download_path)
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
            if attrib_only != True:
                file_exists = os.path.isfile(download_path+'/'+obj_id+'/'+cid+'.file') or os.path.isfile(download_path+'/'+obj_id+'/'+cid+'.dag')      
                if messages.add_assert(file_exists == True, "Could not fild the local file for "+obj_id+":"+cid) == False:
                    return False,messages
        '''
        query = {
            'attrib':obj
        }
        return query,messages
    

    
    @classmethod
    def push_payload_to(cls,ds_remote,decw,obj,download_path,connection_settings):
        # TODO Assert ds_remote is of IPFS Type
        obj_id = obj['self_id']
        messages = ObjectMessages("DsGeneralLocal(for IPFS).push_payload_to")
        obj_cids = []
        obj_cids.append(obj['settings']['ipfs_cid'])
        if 'ipfs_cids' in obj['settings']:
            for path,cid in obj['settings']['ipfs_cids'].items():
                obj_cids.append(cid)

        all_cids =  ds_remote.ipfs_pin_list(decw, connection_settings,refresh=True)
        missing_cids = list(set(obj_cids) - set(all_cids))
        if(len(missing_cids) > 0):
            reupload_cids,upload_messages = cls.upload_ipfs_data(ds_remote,decw,download_path+'/'+obj_id,connection_settings)
            messages.append(upload_messages)
            if messages.add_assert(ds_remote.ipfs_has_cids(decw,obj_cids, connection_settings,refresh=True) == True,
                                "Could not find the file in IPFS post re-upload. Please check "+download_path+'/'+obj_id +" manually",)==False:
                return False,messages
        return len(messages.get_error_messages()) == 0, messages
    
    # TODO remove all hard path requirements from this file
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
    def remove_attrib(cls,filter:dict,download_path:str):
        return UtilFile.remove_attrib(filter,download_path)

    @classmethod
    def remove_payload(cls,filter:dict,download_path:str):
        return UtilFile.remove_payload(filter,download_path)