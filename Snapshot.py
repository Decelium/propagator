import os
import json
import shutil
try:
    from datasource.TpGeneral import TpFacade
    from datasource.TpGeneralLocal import TpGeneralLocal
    from datasource.TpGeneralDecelium import TpGeneralDecelium
    from datasource.TpGeneralDeceliumMirror import TpGeneralDeceliumMirror
    from datasource.TpGeneralLocalMirror import TpGeneralLocalMirror
    from datasource.TpAttrib import TpAttrib
    from datasource.TpIPFS import TpIPFS
    from datasource.TpFile import TpFile
    from Messages import ObjectMessages
    from type.BaseData import BaseData,auto_c
    from datasource.TpGeneral import TpGeneral
except:
    from .datasource.TpGeneral import TpFacade
    from .datasource.TpGeneralLocal import TpGeneralLocal
    from .datasource.TpGeneralDecelium import TpGeneralDecelium
    from .datasource.TpGeneralDeceliumMirror import TpGeneralDeceliumMirror
    from .datasource.TpGeneralLocalMirror import TpGeneralLocalMirror
    from .datasource.TpAttrib import TpAttrib
    from .datasource.TpIPFS import TpIPFS
    from .datasource.TpFile import TpFile
    from .type.BaseData import BaseData,auto_c
    from .Messages import ObjectMessages
    from .datasource.TpGeneral import TpGeneral

import traceback as tb
import decelium_wallet.core as core


class EntityRequestData(BaseData):
    def get_keys(self):
        required = {'self_id': str }
        optional = {'attrib': bool}
        return required, optional    
    
class Snapshot:  
    s_type_map = {
        'ipfs': TpIPFS,
        'file': TpFile,
        'json': TpFile,
        'host': TpAttrib,
        'user': TpFile,
        'node': TpAttrib,
        'directory':TpAttrib
    }
    s_property = ['attrib','payload','']
    s_datasource = ['local','local_mirror','remote','remote_mirror']
    s_datasourceproperty_to_datasource = {'local':'local',
                      'local_attrib':'local',
                      'local_payload':'local',
                      'remote':'remote',
                      'remote_attrib':'remote',
                      'remote_payload':'remote',
                      'remote_mirror':'remote_mirror',
                      'remote_mirror_attrib':'remote_mirror',
                      'remote_mirror_payload':'remote_mirror'}
    @staticmethod
    def get_datasource(type_name:str, datasource_name:str) -> TpGeneral:
        assert type_name in list(Snapshot.s_type_map.keys()), 'Could not find the type name in the registered types '+ str(type_name)
        assert datasource_name in Snapshot.s_datasource, 'could not find a property with name '+ str(datasource_name)
        TheType:TpFacade = Snapshot.s_type_map[type_name]
        return TheType.get_datasource_refac(datasource_name)
    
    @staticmethod
    def resolve_type(decw,obj_id,datasource_name,download_path,cached = True):
        obj = Snapshot.load_file_by_id(decw,obj_id,datasource_name,download_path,cached)
        assert 'file_type' in obj and len(obj['file_type']) > 0, "Seem to have found invalid file_type:" + str(obj)
        return obj['file_type']

    
    @staticmethod
    def get_object_datasource(decw,obj_id:str,datasource_name:str,download_path:str) -> TpGeneral:
        cached = True
        type_name = Snapshot.resolve_type(decw,obj_id,datasource_name,download_path,cached )
        assert type_name != None
        return Snapshot.get_datasource(type_name,datasource_name)
    
    @staticmethod
    def get_general_datasource(datasource_name:str) -> TpGeneral:
        obj_map = {
            'local':TpGeneralLocal,
            'local_mirror':TpGeneralLocalMirror,
            'remote':TpGeneralDecelium,
            'remote_mirror':TpGeneralDeceliumMirror
        }
        assert datasource_name in list(obj_map.keys())
        return obj_map[datasource_name]
        
    @staticmethod
    def format_object_status_json(self_id:str,prefix:str,status:bool,message:list,error:str):
            result = {}
            result[prefix] = status
            result[prefix+"_message"] = message
            result[prefix+"_error"] = error
            return result

    @staticmethod
    def load_file_by_id(decw,obj_id,datasource,download_path,cached = False):
        try:
            assert type(Snapshot.__resolution_cache) == dict
        except:
            Snapshot.__resolution_cache = {}
        if cached == True and obj_id in Snapshot.__resolution_cache:
            return Snapshot.__resolution_cache[obj_id]
            
        if 'local' in datasource:
            obj = TpGeneralLocal.load_entity({'api_key':"UNDEFINED", 'self_id':obj_id, 'attrib':True },download_path)
        elif 'remote_mirror' in datasource:
            obj = TpGeneralDeceliumMirror.load_entity({'api_key':"UNDEFINED", 'self_id':obj_id, 'attrib':True },decw)
        elif 'remote' in datasource:
            obj = TpGeneralDecelium.load_entity({'api_key':"UNDEFINED", 'self_id':obj_id, 'attrib':True },decw)
        else:
            obj= {"error":"Could not identify type in Snapshot.load_file_by_id() "}
        if "self_id" in obj:
            Snapshot.__resolution_cache[obj_id] = obj
        
        return obj

    @staticmethod
    def object_validation_status(decw,obj_id,download_path,connection_settings,datasourceproperty,previous_messages=None,prefix=None):
        result_json = {}
        result_json["self_id"] = obj_id
        messages = ObjectMessages("Snapshot.object_validation_status")
        result = True
        #    return result_json,messages

        if prefix:
            outfix = prefix
        else:
            outfix = datasourceproperty
        obj = Snapshot.load_file_by_id(decw,obj_id,datasourceproperty,download_path)
        if messages.add_assert(not 'error' in obj,"Could not find file by id: "+str(obj)) == False:
            result = False
            result_json = Snapshot.format_object_status_json(obj_id,outfix,result,messages.get_error_messages(),"")
            return result_json,messages      
            
        if messages.add_assert('file_type' in obj,"Object does not have file type: "+str(obj)) == False:
            result = False
            result_json = Snapshot.format_object_status_json(obj_id,outfix,result,messages.get_error_messages(),"")
            return result_json,messages      

        selected_type = obj['file_type']
        assert selected_type in list(Snapshot.s_type_map.keys()), "Selected type not supported"
        assert datasourceproperty in list(Snapshot.s_datasourceproperty_to_datasource.keys()), "datasource_property is not supported: "+datasourceproperty

        # Map something like 'remote_attrib' into 'remote'
        datasource_location = Snapshot.s_datasourceproperty_to_datasource[datasourceproperty]
        #TpDataType:TpFacade = Snapshot.s_type_map[selected_type]
        #TpDatasource:TpGeneral  = TpDataType.get_datasource(datasource_location)
        TpDatasource:TpGeneral = Snapshot.get_datasource(selected_type,datasource_location)
        validation_set = {}

        #assert f"{datatype}.{selected_type}" in type_map, "Could not find the selected datatype: " + f"{datatype}.{selected_type}"
        #TpDatasource:TpGeneral = type_map[f"{datatype}.{selected_type}"]

            
        validation_set = {**validation_set,**{
            datasource_location:{'func':TpDatasource.validate_object},
            f'{datasource_location}_attrib':{'func':TpDatasource.validate_object_attrib},
            f'{datasource_location}_payload':{'func':TpDatasource.validate_object_payload},
        }}

        func = validation_set[datasourceproperty]['func']
        result,messages = func(decw,obj_id,download_path,connection_settings)
        if previous_messages:
            messages.append(previous_messages)

        
        result_json = Snapshot.format_object_status_json(obj_id,outfix,result,messages.get_error_messages(),"")

        return   result_json,messages      

    @staticmethod
    def append_from_remote(decw, connection_settings, download_path, limit=20, offset=0,filter = None, overwrite = False,api_key="undefined",attrib=None):
        if filter == None:
            filter = {'attrib':{'file_type':'ipfs'}}
            file_type = 'ipfs'
            
        local_object_ids = []
        if os.path.exists(download_path):
            local_object_ids = os.listdir(download_path)
        
        found_objs = Snapshot.get_general_datasource("remote").find_batch_objects(decw,offset,limit,filter)

        needed_objs = found_objs
        results = {}
        if len(needed_objs) <= 0:
            return {}
        for obj in needed_objs:
            # print("obj",obj)
            print(f"syncing {obj['self_id']}")
            obj_id = obj['self_id']
            if attrib == True:
                validation_target = 'local_attrib'
            else:
                validation_target = 'local'
            try:
                from_remote_datasource = Snapshot.get_datasource(obj['file_type'],"remote")
                object_results = Snapshot.get_datasource(obj['file_type'],"local").download_object(from_remote_datasource,decw,[obj_id], download_path, connection_settings,overwrite,attrib)
                print(object_results)
                print(obj['file_type'])
                messages_print:ObjectMessages = object_results[obj_id][1]
                result = object_results[obj_id][0]
                    
                if object_results[obj_id][0] == True:
                    print("append_from_remote. NOW DOING VALIDATION " +str(attrib))
                    messages = object_results[obj_id][1]
                    # (decw,obj_id,download_path,connection_settings,datasourceproperty,previous_messages=None,prefix=None):
                    results[obj_id],_ = Snapshot.object_validation_status(decw,obj_id,download_path,connection_settings,validation_target,messages,'local')
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
        decw = None # get_general_datasource(datasource_name:str)
        # file_datasoruce = Snapshot.get_object_datasource(decw,filter['self_id'],"local",download_path)
        file_datasoruce = Snapshot.get_general_datasource("local")
        return file_datasoruce.remove_entity(filter,download_path)

    @auto_c(EntityRequestData)
    def remove_attrib(filter:EntityRequestData,download_path:str):
        assert 'self_id' in filter
        decw = None
        # file_datasoruce = Snapshot.get_object_datasource(decw,filter['self_id'],"local",download_path)
        file_datasoruce = Snapshot.get_general_datasource("local")
        return file_datasoruce.remove_attrib(filter,download_path)

    @auto_c(EntityRequestData)
    def corrupt_attrib(filter:EntityRequestData,download_path:str):
        assert 'self_id' in filter
        decw = None
        file_datasoruce = Snapshot.get_object_datasource(decw,filter['self_id'],"local",download_path)
        return file_datasoruce.corrupt_attrib(filter,download_path)

    #@auto_c(EntityRequestData)
    #def corrupt_attrib(filter:EntityRequestData,download_path:str):
    #    assert 'self_id' in filter
    #    return Snapshot.get_datasource("ipfs",filter,download_path)
    
    @auto_c(EntityRequestData)
    def remove_payload(filter:EntityRequestData,download_path:str):
        assert 'self_id' in filter
        decw = None
        file_datasoruce = Snapshot.get_object_datasource(decw,filter['self_id'],"local",download_path)
        return file_datasoruce.remove_payload(filter,download_path)
    
    @auto_c(EntityRequestData)
    def corrupt_attrib_filename(filter:EntityRequestData,download_path:str):
        assert 'self_id' in filter
        decw = None
        file_datasoruce = Snapshot.get_object_datasource(decw,filter['self_id'],"local",download_path)
        return file_datasoruce.corrupt_attrib_filename(filter,download_path)

    @staticmethod
    @auto_c(EntityRequestData)
    def corrupt_payload(filter:EntityRequestData,download_path:str):
        assert 'self_id' in filter
        decw = None
        file_datasoruce = Snapshot.get_object_datasource(decw,filter['self_id'],"local",download_path)
        return file_datasoruce.corrupt_payload(filter,download_path)
    

    @staticmethod
    def push_to_remote(decw, connection_settings, download_path, limit=20, offset=0,filter = None, overwrite = False,api_key = None,attrib_only=None):
        if api_key == None:
            api_key = decw.dw.pubk("admin")
        messages = ObjectMessages("Snapshot.push_to_remote")
        unfiltered_ids = os.listdir(download_path)
        object_ids = []
        for obj_id in unfiltered_ids:
            if not 'obj-' in obj_id:
                continue
            if type(filter) == dict and 'attrib' in filter and 'self_id' in filter['attrib']:
                if obj_id == filter['attrib']['self_id']:
                    object_ids.append(obj_id)
                    continue
            else:
                object_ids.append(obj_id)
        object_ids = object_ids[offset:offset+limit]
        results = {}

        if len(object_ids) == 0:
            return results        
        for obj_id in object_ids:

            # ---------
            # a) Make sure the remote is missing
            # TODO -- Check for SIMILARITY not just a valid server object. Should push CHANGES up as well.
            remote_result_mirror, remote_validation_messages_mirror = Snapshot.get_object_datasource(decw,obj_id,"remote_mirror",download_path).validate_object(decw,obj_id, download_path, connection_settings)
            
            remote_result, remote_validation_messages = Snapshot.get_object_datasource(decw,obj_id,"remote",download_path).validate_object(decw,obj_id, download_path, connection_settings)
            #if remote_result == True and remote_result_mirror == False:

            
            if remote_result == True and remote_result_mirror == True:
                print("Both remote results are true (Snapshot)")
                results[obj_id]= (True,remote_validation_messages.get_error_messages())
                continue
            print("GOINF FOR RESTORE(Snapshot)")
            # TODO Generalize, and split attrib / payload functions (?) or not (?)
            if attrib_only == True:
                
                # ---------
                # b) Make sure the local is complete (attrib only)
                local_result, local_validation_messages = Snapshot.get_object_datasource(decw,obj_id,"local",download_path).validate_object_attrib(decw,obj_id, download_path, connection_settings)
                if local_result == False: # and remote_result == False:
                    results[obj_id] = (False,local_validation_messages.get_error_messages())
                    continue
                assert local_result == True and remote_result == False
                # ---------
                # Upload metadata (attrib only)
                query,upload_messages = Snapshot.get_object_datasource(decw,obj_id,"local",download_path).upload_object_query(obj_id,download_path,connection_settings,attrib_only)
                messages.append(upload_messages)
                if len(upload_messages.get_error_messages()) > 0:
                    results[obj_id] = (False,messages.get_error_messages())
                    continue

                result = decw.net.restore_attrib({**query,'api_key':api_key,'ignore_mirror':True}) # ** TODO Fix buried credential, which is now expanding as a problem
                if messages.add_assert('error' not in result,"D. Upload did not secceed at all:"+str(result)+ "for object "+str(query))==False:
                    results[obj_id]= (False,messages.get_error_messages())
                    continue
                    
                if messages.add_assert('__entity_restored' in result and result['__entity_restored']==True,"Could not restore the attrib "+str(result))==False:
                    results[obj_id]= (False,messages.get_error_messages())
                    continue

                if messages.add_assert('__mirror_restored' in result and result['__mirror_restored']==False,"The Mirror should have been ignored "+str(result))==False:
                    results[obj_id]= (False,messages.get_error_messages())
                    continue

                results[obj_id] = (True,messages.get_error_messages())
                continue
            # ---------
            # b) Make sure the local is complete
            local_result, local_validation_messages = Snapshot.get_object_datasource(decw,obj_id,"local",download_path).validate_object(decw,obj_id, download_path, connection_settings)
            if local_result == False: # and remote_result == False:
                results[obj_id] = (False,local_validation_messages.get_error_messages())
                continue

            # ---------
            # assert the case (a,b)
            assert local_result == True and (remote_result == False or remote_result_mirror == False)

            # ---------
            # Upload metadata
            query,upload_messages = Snapshot.get_object_datasource(decw,obj_id,"local",download_path).upload_object_query(obj_id,download_path,connection_settings)
            messages.append(upload_messages)
            if len(upload_messages.get_error_messages()) > 0:
                results[obj_id] = (False,messages.get_error_messages())
                continue
            
            obj = Snapshot.get_object_datasource(decw,obj_id,"local",download_path).load_entity({'api_key':api_key,"self_id":obj_id,'attrib':True},download_path)
            if messages.add_assert('error' not in obj,"b. Somehow the local is corrupt. Should be impossible to get this error."+ str(obj))==False:
                results[obj_id]= (False,messages.get_error_messages())
                continue
            # TODO - Check if payload is missing using validate remote
            obj_cids = []
            if remote_result == False:                
                # We toss up a restore, that may fail, as some files require attrib-first restore, and others require payload first restore.
                # Here I am testing doing attrib - payload - attrib (again) which should be able to resotre any general entity.
                ignore_result = decw.net.restore_attrib({**query,'api_key':api_key}) # ** TODO Fix buried credential 

                results[obj_id]= (False,messages.get_error_messages())
                # ---------
                # Upload cids
                ds_local = Snapshot.get_object_datasource(decw,obj_id,"local",download_path)
                ds_remote = Snapshot.get_object_datasource(decw,obj_id,"remote",download_path)
                success, payload_messages = ds_local.push_payload_to(ds_remote,decw,obj,download_path,connection_settings)
                if len(payload_messages.get_error_messages()) > 0:
                    results[obj_id] = (False,payload_messages.get_error_messages() + ['a. failure in Snapshot.ds_local.push_payload_to()'])
                    continue
                if success == False:
                    results[obj_id] = (False,payload_messages.get_error_messages() + ['b. failure in Snapshot.ds_local.push_payload_to()'])
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
            
            remote_result, remote_validation_messages = Snapshot.get_object_datasource(decw,obj_id,"remote",download_path).validate_object(decw,obj_id, download_path, connection_settings)
            messages.append(remote_validation_messages)
            # results[obj_id]= (remote_result,messages.get_error_messages())
            if messages.add_assert(remote_result == True,"Could not complete proper remote restore")==False:
                results[obj_id]= (False,messages.get_error_messages())
                continue

            results[obj_id]= (remote_result,messages.get_error_messages())

            # TODO validate the mirror as well

        return results    
    
    @staticmethod
    # TODO / Verify
    def pull_from_remote(decw, connection_settings, download_path,limit=20, offset=0,overwrite=False,api_key="undefined",attrib=None):
        object_ids = os.listdir(download_path)
        object_ids = object_ids[offset:offset+limit]
        found_objs = {}
        if len(object_ids) == 0:
            return found_objs
        for obj_id in object_ids:
            filter = {'attrib':{'self_id':obj_id}}
            object_results = Snapshot.append_from_remote(decw, connection_settings, download_path,1, 0,filter,overwrite,api_key,attrib)      
            found_objs.update(object_results)       
        return found_objs
    
    @staticmethod
    def validate_snapshot(decw, connection_settings, download_path,limit=20, offset=0,overwrite=False):
        if not os.path.exists(download_path):
            os.mkdir(download_path)
        object_ids = os.listdir(download_path)
        found_objs = []
        results = {}
        current_offset = 0
        for obj_id in object_ids:
            if not 'obj-' in obj_id:
                continue

            if current_offset < offset:
                current_offset += 1
                continue
            results[obj_id] = {'self_id':obj_id}       
            local_results,_ = Snapshot.object_validation_status(decw,obj_id,download_path,connection_settings,'local')
            remote_results,_ = Snapshot.object_validation_status(decw,obj_id,download_path,connection_settings,'remote')
            remote_mirror_results,_ = Snapshot.object_validation_status(decw,obj_id,download_path,connection_settings,'remote_mirror')
            results[obj_id].update(local_results)
            results[obj_id].update(remote_results)
            results[obj_id].update(remote_mirror_results)
            current_offset += 1
            if current_offset >= limit+offset:
                break
        return results