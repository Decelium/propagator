import os
import json
import shutil
from Migrator import Migrator
from datasource.TpIPFSDecelium import TpIPFSDecelium
from datasource.TpIPFSLocal import TpIPFSLocal

from Messages import ObjectMessages
import traceback as tb

class Snapshot:  

    @staticmethod
    def format_object_status_json(self_id:str,prefix:str,status:bool,message:list,error:str):
            result = {}
            result[prefix] = status
            result[prefix+"_message"] = message
            result[prefix+"_error"] = tb.format_exc()
            return result
    
    @staticmethod
    def object_validation_status(decw,obj_id,download_path,connection_settings,datasource,previous_messages=None):
        result_json = {}
        result_json["self_id"] = obj_id
        validation_set = {'local':{'func':TpIPFSLocal.validate_local_object,
                                   'prefix':'local'
                                   },
                           'remote':{'func':TpIPFSDecelium.validate_remote_object,
                                   'prefix':'remote'
                                     }
                           }
        prefix = validation_set[datasource]['prefix']
        func = validation_set[datasource]['func']
        #try:
        result,messages = func(decw,obj_id,download_path,connection_settings)
        if previous_messages:
            messages.append(previous_messages)
        result_json = Snapshot.format_object_status_json(obj_id,prefix,result,messages.get_error_messages(),"")
        return   result_json,messages      
        #except:
        #    result_json = Snapshot.format_object_status_json(obj_id,prefix,False,previous_messages,tb.format_exc())
        #    return   result_json,messages      

    @staticmethod
    def append_from_remote(decw, connection_settings, download_path, limit=20, offset=0,filter = None, overwrite = False):
        if filter == None:
            filter = {'attrib':{'file_type':'ipfs'}}
        local_object_ids = []

        if os.path.exists(download_path):
            local_object_ids = os.listdir(download_path)

        found_objs = TpIPFSDecelium.find_batch_object_ids(decw,offset,limit,filter)
        needed_objs = found_objs
        results = {}
        if len(needed_objs) <= 0:
            return {}
        
        for obj_id in needed_objs:

            #if (not os.path.exists(download_path+'/'+obj_id)) or overwrite==True:
            try:
                object_results = TpIPFSLocal.download_object(TpIPFSDecelium,decw,[obj_id], download_path, connection_settings,overwrite )
                messages_print:ObjectMessages = object_results[obj_id][1]
                result = object_results[obj_id][0]
                if object_results[obj_id][0] == True:
                    messages = object_results[obj_id][1]
                    results[obj_id],_ = Snapshot.object_validation_status(decw,obj_id,download_path,connection_settings,'local',messages)
                else:
                    result = False
                    messages = object_results[obj_id][1]
                    results[obj_id] = Snapshot.format_object_status_json(obj_id,'local',result,messages.get_error_messages(),"")

            except:

                results[obj_id] = Snapshot.format_object_status_json(obj_id,'local',False,[],tb.format_exc())
            #if overwrite == False:
            #    print("Validating "+ obj_id)
            #    results[obj_id],_ = Snapshot.object_validation_status(decw,obj_id,download_path,connection_settings,'local')
        return results

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

    @staticmethod
    def remove_entity(filter,download_path):
        assert 'self_id' in filter
        assert 'attrib' in filter and filter['attrib'] == True
        try:
            with open(download_path+'/'+filter['self_id']+'/object.json','r') as f:
                obj_attrib = json.loads(f.read())
            return obj_attrib
        except:
            return {'error':"Could not read a valid object.json from "+download_path+'/'+filter['self_id']+'/object.json'}


    @staticmethod
    def push_to_remote(decw, connection_settings, download_path, limit=20, offset=0,filter = None, overwrite = False):
        # def push_to_remote(decw,api_key, connection_settings, download_path,limit=20, offset=0):
        api_key = decw.dw.pubk("admin")

        messages = ObjectMessages("Snapshot.push_to_remote")
        object_ids = os.listdir(download_path)
        object_ids = object_ids[offset:offset+limit]
        results = {}
        if len(object_ids) == 0:
            return results        
        
        for obj_id in object_ids:
            
            # ---------
            # a) Make sure the remote is missing
            # TODO -- Check for SIMILARITY not just a valid server object. Should push CHANGES up as well.
            remote_result, remote_validation_messages = TpIPFSDecelium.validate_remote_object(decw,obj_id, download_path, connection_settings)
            if remote_result == True:
                results[obj_id]= (True,remote_validation_messages.get_error_messages())
                continue

            # ---------
            # b) Make sure the local is complete
            local_result, local_validation_messages = TpIPFSLocal.validate_local_object(decw,obj_id, download_path, connection_settings)
            if local_result == False: # and remote_result == False:
                results[obj_id] = (False,local_validation_messages.get_error_messages())
                continue

            # ---------
            # assert the case (a,b)
            assert local_result == True and remote_result == False

            # ---------
            # Upload metadata
            query,upload_messages = TpIPFSLocal.upload_object_query(obj_id,download_path,connection_settings)
            messages.append(upload_messages)
            if len(upload_messages.get_error_messages()) > 0:
                results[obj_id] = (False,messages)
                continue
            
            result = decw.net.restore_attrib(decw.dw.sr({**query,'api_key':api_key},["admin"])) # ** TODO Fix buried credential 
            if messages.add_assert('error' not in result,"a. Upload did not secceed at all:"+str(result)+ "for object "+str(query))==False:
                results[obj_id]= (False,messages.get_error_messages())
                continue

            obj = TpIPFSDecelium.load_entity({'api_key':api_key,"self_id":obj_id,'attrib':True},decw)
            if messages.add_assert('error' not in obj,"b. Upload did not secceed at all:"+ str(obj))==False:
                results[obj_id]= (False,messages.get_error_messages())
                continue

            obj_cids = []
            remote_result, remote_validation_messages = TpIPFSDecelium.validate_remote_object(decw,obj_id, download_path, connection_settings)
            messages.append(remote_validation_messages)
            if remote_result == False:
                results[obj_id]= (False,messages.get_error_messages())
                # ---------
                # Upload cids

                for path,cid in obj['settings']['ipfs_cids'].items():
                    obj_cids.append(cid)

                all_cids =  TpIPFSDecelium.ipfs_pin_list(decw, connection_settings,refresh=True)
                missing_cids = list(set(obj_cids) - set(all_cids))
                if(len(missing_cids) > 0):
                    reupload_cids,upload_messages = TpIPFSLocal.upload_ipfs_data(TpIPFSDecelium,decw,download_path+'/'+obj_id,connection_settings)
                    messages.append(upload_messages)
                    if messages.add_assert(TpIPFSDecelium.ipfs_has_cids(decw,obj_cids, connection_settings,refresh=True) == True,
                                        "Could not find the file in IPFS post re-upload. Please check "+download_path+'/'+obj_id +" manually",)==False:
                        results[obj_id]= (False,messages.get_error_messages())
                        continue

            # ---------
            # Verify Upload was successful
            remote_result, remote_validation_messages = TpIPFSDecelium.validate_remote_object(decw,obj_id, download_path, connection_settings)
            messages.append(remote_validation_messages)
            results[obj_id]= (remote_result,remote_validation_messages.get_error_messages())

        return results
    
    
    @staticmethod
    # TODO / Verify
    def pull_from_remote(decw, connection_settings, download_path,limit=20, offset=0,overwrite=False):
        object_ids = os.listdir(download_path)
        object_ids = object_ids[offset:offset+limit]
        found_objs = {}
        if len(object_ids) == 0:
            return found_objs
        for obj_id in object_ids:
            filter = {'attrib':{'self_id':obj_id}}
            object_results = Snapshot.append_from_remote(decw, connection_settings, download_path,1, 0,filter,overwrite)      
            found_objs.update(object_results)       
        return found_objs
    
    @staticmethod
    def validate_snapshot(decw, connection_settings, download_path,limit=20, offset=0,overwrite=False):
        object_ids = os.listdir(download_path)
        found_objs = []
        results = {}
        current_offset = 0
        for obj_id in object_ids:
            if current_offset < offset:
                current_offset += 1
                continue
            results[obj_id] = {'self_id':obj_id}       
            local_results,_ = Snapshot.object_validation_status(decw,obj_id,download_path,connection_settings,'local')
            remote_results,_ = Snapshot.object_validation_status(decw,obj_id,download_path,connection_settings,'remote')
            results[obj_id].update(local_results)
            results[obj_id].update(remote_results)

            current_offset += 1
            if current_offset >= limit+offset:
                break
        return results
    
