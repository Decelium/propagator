import decelium_wallet.core as core
import os
import json
import pprint
import pandas
from Messages import ObjectMessages
from datasource.TpIPFSDecelium import TpIPFSDecelium
from datasource.TpIPFSLocal import TpIPFSLocal
from datasource.EntityData import EntityData

import cid
import multihash
import traceback as tb
import hashlib
# - Remove all raw IPFS / Decelium code

class Migrator():

    @classmethod        
    def __merge_attrib_from_remote(cls,decw,obj_id,download_path, overwrite):
        # Load local. If the local has a problem
        # TODO - Validate the object before commiting to a local merge. As if a Decelium miner is broken, one could end up destroying a backup.
        # TODO - Handle merges both ways (could be used by push and pull as an underlying mechanism)

        remote_obj = TpIPFSDecelium.load_entity({'api_key':'UNDEFINED', 'self_id':obj_id,'attrib':True},decw)

        local_obj = TpIPFSLocal.load_entity({'api_key':'UNDEFINED', 'self_id':obj_id,'attrib':True},download_path)
        file_path = os.path.join(download_path,obj_id,'object.json')
        # Is the local accurate?
        local_is_valid = TpIPFSLocal.compare_file_hash(file_path)
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

            TpIPFSLocal.overwrite_file_hash(file_path)
            return True,merged_object,merge_messages
        
        if do_write == False and merged_object:
            return True,merged_object,merge_messages
        raise Exception("Should never reach the end of this function.")
    
    @classmethod
    def __merge_payload_from_remote(cls,decw,obj,download_path,connection_settings, overwrite):
        merge_messages = ObjectMessages("Migrator.__merge_payload_from_remote(for obj_id)"+str(obj['self_id']) )

        new_cids = [obj['settings']['ipfs_cid']]
        if 'ipfs_cids' in obj['settings']:
            for cid in obj['settings']['ipfs_cids'].values():
                new_cids.append(cid)
        
        result = TpIPFSDecelium.download_ipfs_data(TpIPFSLocal,decw,new_cids, download_path+'/'+obj['self_id'], connection_settings,overwrite)
        return result

    @classmethod
    def download_object(cls,decw,object_ids,download_path,connection_settings, overwrite=False):
        if type(object_ids) == str:
            object_ids = [object_ids]
        results = {}
        for obj_id in object_ids:
            messages = ObjectMessages("Migrator.download_object(for {obj_id})")
            try:
                success,merged_object,merge_messages = cls.__merge_attrib_from_remote(decw,obj_id,download_path, overwrite)
                messages.append(merge_messages)
                if success:
                    if messages.add_assert(merged_object['self_id'] == obj_id,"There is a serious consistency problem with the local DB. Halt now" ) == False:
                        raise Exception("Halt Now. The data is corrupt.")
                    cls.__merge_payload_from_remote(decw,merged_object,download_path,connection_settings, overwrite)
                    results[obj_id] = (True,messages)
                else:
                    results[obj_id] = (False,messages)
            except:
                exc = tb.format_exc()
                if not os.path.exists(download_path+'/'+obj_id):
                    os.makedirs(download_path+'/'+obj_id)
               
                with open(download_path+'/'+obj_id+'/object_error.txt','w') as f:
                    f.write(exc)
                messages.add_assert(False,"Exception encountered for "+obj_id+": "+exc ) 
                results[obj_id] = (False,messages)
        return results
    '''
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
    '''

    @classmethod
    def upload_object_query(cls,decw,obj_id,download_path,connection_settings):
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