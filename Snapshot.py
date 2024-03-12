import os
import json
import shutil
from Migrator import Migrator
class Snapshot:

    #@staticmethod
    #def init_snapshot(download_path,filter= None):
    #    if filter == None:
    #        filter = {'attrib':{'file_type':'ipfs'}}
    #    assert type(snapshot_desc["filter"]) == dict #
    #
    #    snapshot_desc = {"filter":filter}
    #    if not os.path.exists(os.path.join(download_path,"snapshot.json")):
    #        with open(os.path.join(download_path,"snapshot.json"),'w') as f:
    #            f.write(json.dumps(snapshot_desc))
    #    with open(os.path.join(download_path,"snapshot.json"),'r') as f:
    #        snapshot_desc = json.loads(f.read())
    #    assert "filter" in snapshot_desc
    #    return snapshot_desc
    
    @staticmethod
    def append_from_remote(decw, connection_settings, download_path, limit=20, offset=0,filter = None, overwrite = False):
        if filter == None:
            filter = {'attrib':{'file_type':'ipfs'}}

        found_objs = Migrator.find_batch_object_ids(decw,offset,limit,filter)
        results = {}
        print("append_from_remote 2")
        if len(found_objs) <= 0:
            return {}
        
        for obj_id in found_objs:
            result = {'self_id':obj_id,'local':None,'remote':None}
            if (not os.path.exists(download_path+'/'+obj_id)) or overwrite==True:
                print("saving",obj_id)
                try:
                    object_results = Migrator.download_object(decw,[obj_id], download_path, connection_settings,overwrite )
                    object_results[obj_id]
                    result['local'] = object_results[obj_id]
                    result['remote'] = object_results[obj_id]
                    results[obj_id] = result
                except:
                    result['local'] = False
                    result['remote'] = False
                    results[obj_id] = result
            if overwrite == False:
                print("Validating "+ obj_id)
                try:
                    result['local'] = Migrator.validate_local_object(decw,obj_id, download_path, connection_settings)
                    result['remote'] = None
                    results[obj_id] = result
                except:
                    result['local'] = False
                    result['remote'] = None
                    results[obj_id] = result
        return results

    @staticmethod
    def load_entity(filter,download_path):
        assert 'self_id' in filter
        assert 'attrib' in filter and filter['attrib'] == True
        with open(download_path+'/'+filter['self_id']+'/object.json','r') as f:
            obj_attrib = json.loads(f.read())
        return obj_attrib

    @staticmethod
    def push_to_remote(decw,api_key, connection_settings, download_path,limit=20, offset=0):
        object_ids = os.listdir(download_path)
        # (later) TODO Implement limit and offset
        ipfs_cids = []
        all_cids =  Migrator.ipfs_pin_list( connection_settings)
        for obj_id in object_ids:
            obj = decw.net.download_entity({'api_key':api_key,"self_id":obj_id,'attrib':True})
            if type(obj) == dict and 'error' in obj:
                query = Migrator.upload_object_query(decw,obj_id,download_path,connection_settings)
                result = decw.net.create_entity(decw.dw.sr({**query,'api_key':api_key},["admin"])) # ** TODO Fix buried credential 
                assert 'obj-' in result
                obj = decw.net.download_entity({'api_key':api_key,"self_id":obj_id,'attrib':True})
            assert 'settings' in obj
            assert 'ipfs_cid' in obj['settings']
            assert 'ipfs_cids' in obj['settings']
            #obj_cids = [obj['settings']['ipfs_cid']]
            obj_cids = [obj['settings']['ipfs_cid']]
            for path,cid in obj['settings']['ipfs_cids'].items():
                obj_cids.append(cid)
            missing_cids = list(set(all_cids) - set(obj_cids))
            if(len(missing_cids) > 0):
                Migrator.upload_ipfs_data(decw,download_path+'/'+obj_id,connection_settings)
                assert Migrator.ipfs_has_cids(decw,obj_cids, connection_settings) == True

        return missing_cids

    @staticmethod
    def pull_from_remote(decw, connection_settings, download_path,limit=20, offset=0,overwrite=False):
        object_ids = os.listdir(download_path)
        found_objs = []
        current_offset = 0
        for obj_id in object_ids:
            if current_offset < offset:
                current_offset += 1
                continue            
            filter = {'attrib':{'self_id':obj_id}}
            found_objs = found_objs + Snapshot.append_from_remote(decw, connection_settings, download_path,1, 0,filter,overwrite)            
            current_offset += 1
        return found_objs
    
    @staticmethod
    def validate_snapshot(decw, connection_settings, download_path,limit=20, offset=0,overwrite=False):
        object_ids = os.listdir(download_path)
        found_objs = []
        results = {}
        # print(object_ids)
        current_offset = 0
        for obj_id in object_ids:
            if current_offset < offset:
                current_offset += 1
                continue               
            print (obj_id)
            results[obj_id] = {'self_id':obj_id,'local':None,'remote':None}          
            '''  
            try:
                results[obj_id]["local"] = Migrator.validate_local_object(decw,obj_id,download_path,connection_settings)
                results[obj_id]["local_message"] = "Local Synchronized successfully."
                results[obj_id]["local_error"] = ""
            except:
                results[obj_id]["local"] = False
                results[obj_id]["local_message"] = "Local Errror"
                import traceback as tb
                results[obj_id]["local_error"] = tb.format_exc()
            '''
            try:
                results[obj_id]["remote"] = Migrator.validate_remote_object(decw,obj_id,download_path,connection_settings)
                results[obj_id]["remote_message"] = "Remote Synchronized successfully."
                results[obj_id]["remote_error"] = ""
            except:
                results[obj_id]["remote"] = False
                results[obj_id]["remote_message"] = "Connection Errror"
                import traceback as tb
                results[obj_id]["remote_error"] = tb.format_exc()
            current_offset += 1
            if current_offset >= limit+offset:
                break
        return results

