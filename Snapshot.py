import os
import json
import shutil
from Migrator import Migrator
class Snapshot:
    @staticmethod
    def append_from_remote(decw, connection_settings, download_path, limit=20, offset=0,filter = None, overwrite = False):
        if filter == None:
            filter = {'attrib':{'file_type':'ipfs'}}
        found_objs = Migrator.find_batch_object_ids(decw,offset,limit,filter)
        if len(found_objs) <= 0:
            return []
        for obj_id in found_objs:
            print("append_from_remote FOUND",obj_id,download_path)
            print(overwrite)
            if (not os.path.exists(download_path+'/'+obj_id)) or overwrite==True:
                print("saving",obj_id)
                Migrator.download_object(decw,[obj_id], download_path, connection_settings,overwrite )
            if overwrite == False:
                assert Migrator.validate_backedup_object(decw,obj_id, download_path, connection_settings) == True
        return found_objs

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

    def pull_from_remote(decw, connection_settings, download_path,limit=20, offset=0,overwrite=False):
        object_ids = os.listdir(download_path)
        found_objs = []
        for obj_id in object_ids:
            print("Searching for ,"+obj_id)
            filter = {'attrib':{'self_id':obj_id}}
            found_objs = found_objs + Snapshot.append_from_remote(decw, connection_settings, download_path,limit, offset,filter,overwrite)
        return found_objs