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
                success,merged_object,merge_messages = TpIPFSLocal.merge_attrib_from_remote(TpIPFSDecelium,decw,obj_id,download_path, overwrite)
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