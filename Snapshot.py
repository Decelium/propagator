import os
import json
import shutil
try:
    from datasource.TpIPFSDecelium import TpIPFSDecelium
    from datasource.TpIPFSLocal import TpIPFSLocal
    from Messages import ObjectMessages
    from type.BaseData import BaseData,auto_c

except:
    from .datasource.TpIPFSDecelium import TpIPFSDecelium
    from .datasource.TpIPFSLocal import TpIPFSLocal
    from .type.BaseData import BaseData,auto_c
    from .Messages import ObjectMessages

import traceback as tb
import decelium_wallet.core as core


class EntityRequestData(BaseData):
    def get_keys(self):
        required = {'self_id': str }
        optional = {'attrib': bool}
        return required, optional    
    

class Snapshot:  
    @staticmethod
    def format_object_status_json(self_id:str,prefix:str,status:bool,message:list,error:str):
            result = {}
            result[prefix] = status
            result[prefix+"_message"] = message
            result[prefix+"_error"] = error
            return result
    
    @staticmethod
    def object_validation_status(decw,obj_id,download_path,connection_settings,datasource,previous_messages=None,prefix=None):
        result_json = {}
        result_json["self_id"] = obj_id
        # entity_success,entity_messages = cls.validate_local_object_entity(decw,object_id,download_path,connection_settings)
        # payload_success,payload_messages = cls.validate_local_object_payload(decw,object_id,download_path,connection_settings)        
        #
        # TODO - Consolidate and refactor function users (?)
        validation_set = {
            'local':{'func':TpIPFSLocal.validate_local_object,
                    'prefix':'local'
                    },
            'local_attrib':{'func':TpIPFSLocal.validate_local_object_attrib,
                    'prefix':'local_attrib'
                    },
            'local_payload':{'func':TpIPFSLocal.validate_local_object_payload,
                    'prefix':'local_payload'
                    },
            'remote':{'func':TpIPFSDecelium.validate_remote_object,
                    'prefix':'remote'
                    },
            'remote_mirror':{'func':TpIPFSDecelium.validate_remote_object_mirror,
                    'prefix':'remote_mirror'
                    },                    
            'remote_attrib':{'func':TpIPFSDecelium.validate_remote_object_attrib,
                    'prefix':'remote_attrib'
                        },
            'remote_payload':{'func':TpIPFSDecelium.validate_remote_object_payload,
                    'prefix':'remote_payload'
                        },
            'remote_attrib_mirror':{'func':TpIPFSDecelium.validate_remote_object_attrib_mirror,
                    'prefix':'remote_entity_attrib'
                        },
            'remote_payload_mirror':{'func':TpIPFSDecelium.validate_remote_object_payload_mirror,
                    'prefix':'remote_payload_mirror'
                        }                        
        }
        if prefix == None:
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
    @auto_c(EntityRequestData)
    def load_entity(filter:EntityRequestData,download_path:str):
        assert 'attrib' in filter and filter['attrib'] == True
        try:
            with open(download_path+'/'+filter['self_id']+'/object.json','r') as f:
                obj_attrib = json.loads(f.read())
            return obj_attrib
        except:
            return {'error':"Could not read a valid object.json from "+download_path+'/'+filter['self_id']+'/object.json'}

    @staticmethod
    @auto_c(EntityRequestData)
    def remove_entity(filter:EntityRequestData,download_path:str):
        assert 'self_id' in filter
        return TpIPFSLocal.remove_entity(filter,download_path)

    @staticmethod
    @auto_c(EntityRequestData)
    def remove_attrib(filter:EntityRequestData,download_path:str):
        assert 'self_id' in filter
        return TpIPFSLocal.remove_attrib(filter,download_path)

    @staticmethod
    @auto_c(EntityRequestData)
    def remove_payload(filter:EntityRequestData,download_path:str):
        assert 'self_id' in filter
        return TpIPFSLocal.remove_payload(filter,download_path)

    @staticmethod
    @auto_c(EntityRequestData)
    def corrupt_attrib(filter:EntityRequestData,download_path:str):
        assert 'self_id' in filter
        return TpIPFSLocal.corrupt_attrib(filter,download_path)

    @staticmethod
    @auto_c(EntityRequestData)
    def corrupt_attrib_filename(filter:EntityRequestData,download_path:str):
        assert 'self_id' in filter
        return TpIPFSLocal.corrupt_attrib_filename(filter,download_path)

    @staticmethod
    @auto_c(EntityRequestData)
    def corrupt_payload(filter:EntityRequestData,download_path:str):
        assert 'self_id' in filter
        return TpIPFSLocal.corrupt_payload(filter,download_path)
    
    @staticmethod
    def push_to_remote(decw, connection_settings, download_path, limit=20, offset=0,filter = None, overwrite = False,api_key = None,attrib_only=None):
        print("Snapshot.push_to_remote.download_path")
        if api_key == None:
            api_key = decw.dw.pubk("admin")
        messages = ObjectMessages("Snapshot.push_to_remote")
        print("Snapshot.push_to_remote.download_path")
        print(download_path)
        unfiltered_ids = os.listdir(download_path)
        object_ids = []
        for obj_id in unfiltered_ids:
            print("STAGE 3 "+obj_id)
            if not 'obj-' in obj_id:
                continue
            if type(filter) == dict and 'attrib' in filter and 'self_id' in filter['attrib']:
                if obj_id == filter['attrib']['self_id']:
                    object_ids.append(obj_id)
                    continue
            else:
                object_ids.append(obj_id)
        print("STAGE 1 Filtered Objs")
        print(object_ids)
        object_ids = object_ids[offset:offset+limit]
        results = {}
        if len(object_ids) == 0:
            return results        
        print("STAGE 2")
        print(object_ids)
        for obj_id in object_ids:
            print("EXECUTING IN: "+obj_id)            
            # ---------
            # a) Make sure the remote is missing
            # TODO -- Check for SIMILARITY not just a valid server object. Should push CHANGES up as well.
            remote_result, remote_validation_messages = TpIPFSDecelium.validate_remote_object(decw,obj_id, download_path, connection_settings)
            if remote_result == True:
                results[obj_id]= (True,remote_validation_messages.get_error_messages())
                continue
            # TODO Generalize, and split attrib / payload functions (?) or not (?)
            if attrib_only == True:
                # ---------
                # b) Make sure the local is complete (attrib only)
                local_result, local_validation_messages = TpIPFSLocal.validate_local_object_attrib(decw,obj_id, download_path, connection_settings)
                if local_result == False: # and remote_result == False:
                    results[obj_id] = (False,local_validation_messages.get_error_messages())
                    continue
                assert local_result == True and remote_result == False
                # ---------
                # Upload metadata (attrib only)
                query,upload_messages = TpIPFSLocal.upload_object_query(obj_id,download_path,connection_settings,attrib_only)
                messages.append(upload_messages)
                if len(upload_messages.get_error_messages()) > 0:
                    results[obj_id] = (False,messages.get_error_messages())
                    continue

                result = decw.net.restore_attrib({**query,'api_key':api_key,'ignore_mirror':True}) # ** TODO Fix buried credential, which is now expanding as a problem
                print("-------Z1 FINISHED PUSH ATTRIB")
                if messages.add_assert('error' not in result,"D. Upload did not secceed at all:"+str(result)+ "for object "+str(query))==False:
                    results[obj_id]= (False,messages.get_error_messages())
                    continue
                    
                print("-------Z2 FINISHED PUSH ATTRIB")
                if messages.add_assert('__entity_restored' in result and result['__entity_restored']==True,"Could not restore the attrib "+str(result))==False:
                    results[obj_id]= (False,messages.get_error_messages())
                    continue

                print("-------Z3 FINISHED PUSH ATTRIB")
                if messages.add_assert('__mirror_restored' in result and result['__mirror_restored']==False,"The Mirror should have been ignored "+str(result))==False:
                    results[obj_id]= (False,messages.get_error_messages())
                    continue
                

                
                print("-------Z4 FINISHED PUSH ATTRIB")
                results[obj_id] = (True,messages.get_error_messages())
                print("-------ENDED PUSH ATTRIB")
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
                results[obj_id] = (False,messages.get_error_messages())
                continue
            
            obj = TpIPFSLocal.load_entity({'api_key':api_key,"self_id":obj_id,'attrib':True},download_path)
            if messages.add_assert('error' not in obj,"b. Somehow the local is corrupt. Should be impossible to get this error."+ str(obj))==False:
                results[obj_id]= (False,messages.get_error_messages())
                continue
            # TODO - Check if payload is missing using validate remote
            obj_cids = []
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
            # TODO - Check if attrib is missing before calling repair attrib
            # TODO - Restore attrib will also restore the mirror as needed.
            # result = decw.net.restore_attrib(decw.dw.sr({**query,'api_key':api_key},["admin"])) # ** TODO Fix buried credential 
            result = decw.net.restore_attrib({**query,'api_key':api_key}) # ** TODO Fix buried credential 
            
            if messages.add_assert('error' not in result,"a. Upload did not secceed at all:"+str(result)+ "for object "+str(query))==False:
                results[obj_id]= (False,messages.get_error_messages())
                continue
            if '__mirror_restored' in result and type(result['__mirror_restored'])==dict:
                if messages.add_assert('error' not in result['__mirror_restored'],"b. Upload did not secceed at all:"+str(result)+ "for object "+str(query))==False:
                    results[obj_id]= (False,messages.get_error_messages())
                    continue


            # ---------
            # Verify Upload was successful
            remote_result, remote_validation_messages = TpIPFSDecelium.validate_remote_object(decw,obj_id, download_path, connection_settings)
            messages.append(remote_validation_messages)
            # results[obj_id]= (remote_result,messages.get_error_messages())
            if messages.add_assert(remote_result == True,"Could not complete proper remote restore")==False:
                results[obj_id]= (False,messages.get_error_messages())
                continue

            results[obj_id]= (remote_result,messages.get_error_messages())

            # TODO validate the mirror as well

        print("ALL DONE IN PUSH")
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
    
