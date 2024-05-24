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
from .TpGeneral import TpFacade
from .TpGeneralDecelium import TpGeneralDecelium
from .TpGeneralDeceliumMirror import TpGeneralDeceliumMirror
from .TpGeneralLocal import TpGeneralLocal

class TpFile(TpFacade):

    class Decelium(TpGeneralDecelium):
        @classmethod
        def validate_object(cls,decw,object_id,download_path,connection_settings,obj_remote = None):
            entity_success,entity_messages = cls.validate_object_attrib(decw,object_id,download_path,connection_settings)
            payload_success,payload_messages = cls.validate_object_payload(decw,object_id,download_path,connection_settings)
            entity_messages:ObjectMessages = entity_messages
            all_messages:ObjectMessages = payload_messages
            all_messages.append(entity_messages)
            return entity_success and payload_success,all_messages

        @classmethod
        def validate_object_attrib(cls,decw,object_id,download_path,connection_settings,obj_remote = None):
            messages = ObjectMessages("TpFileDecelium.validate_object_entity_mirror(for {"+object_id+"})")
            obj_valid = decw.net.validate_entity_hash( {'api_key':'UNDEFINED', 'self_id':object_id})
            if messages.add_assert(obj_valid == True, f"validate_entity_hash({object_id}) seems to be invalid, as reported by DB validate_object_entity_mirror:"+str(obj_valid)) == False:
                return False, messages
            return len(messages.get_error_messages()) == 0, messages      

        @classmethod
        def validate_object_payload(cls,decw,object_id,download_path,connection_settings,obj_remote = None):
            messages = ObjectMessages("TpFileDecelium.validate_object_payload(for {"+object_id+"})")
            if obj_remote == None:
                obj_remote = decw.net.download_entity( {'api_key':'UNDEFINED', 'self_id':object_id,'attrib':True})
            print("LOW LEVEL TpGeneralDecelium.validate_object_payload")

            obj_valid = decw.net.validate_entity_hash( {'api_key':'UNDEFINED', 'self_id':object_id})
            if messages.add_assert(obj_valid == True, f"B. validate_entity_hash({object_id}) seems to have an invalid hash, as reported by DB validate_object_entity:"+str(obj_valid)) == False:
                return False, messages
            print("TpFileDecelium.validate_object_payload: ")
            print(json.dumps(obj_remote,indent=1)) 

            for k in ['self_id','parent_id','dir_name','settings']:
                if messages.add_assert(k in obj_remote and obj_remote[k] != None, "missing {k} for {object_id}") == False:
                    return False, messages
            
            messages.add_assert('region' in obj_remote['settings'], "missing settings.region for {object_id}")
            messages.add_assert('bucket' in obj_remote['settings'], "missing settings.region for {object_id}")
            payload_data = decw.net.download_entity( {'api_key':'UNDEFINED', 'self_id':object_id})

            #TODO -- Add in a standard payload / hash verification method across files for verifying payloads

            return len(messages.get_error_messages()) == 0, messages  

    class DeceliumMirror(TpGeneralDeceliumMirror):
        @classmethod
        def validate_object(cls,decw,object_id,download_path,connection_settings,obj_remote = None):
            entity_success,entity_messages = cls.validate_object_attrib(decw,object_id,download_path,connection_settings)
            payload_success,payload_messages = cls.validate_object_payload(decw,object_id,download_path,connection_settings)
            entity_messages:ObjectMessages = entity_messages
            all_messages:ObjectMessages = payload_messages
            all_messages.append(entity_messages)
            return entity_success and payload_success,all_messages

        @classmethod
        def validate_object_attrib(cls,decw,object_id,download_path,connection_settings,obj_remote = None):
            messages = ObjectMessages("TpIPFSDeceliumMirror.validate_object_entity_mirror(for {"+object_id+"})")
            obj_valid = decw.net.validate_entity_hash( {'api_key':'UNDEFINED', 'self_id':object_id,'mirror':True})
            if messages.add_assert(obj_valid == True, f"validate_entity_hash({object_id}) seems to be invalid, as reported by DB validate_object_entity_mirror:"+str(obj_valid)) == False:
                return False, messages
            return len(messages.get_error_messages()) == 0, messages      

        @classmethod
        def validate_object_payload(cls,decw,object_id,download_path,connection_settings,obj_remote = None):
            messages = ObjectMessages("TpIPFSDeceliumMirror.validate_object_entity_payload(for {"+object_id+"})")
            obj_valid = decw.net.validate_payload( {'api_key':'UNDEFINED', 'self_id':object_id,'mirror':True})
            if messages.add_assert(obj_valid == True, f"{object_id} seems to be invalid, as reported by DB validate_object_payload_mirror:"+str(obj_valid)) == False:
                return False, messages
            return len(messages.get_error_messages()) == 0, messages    

    class Local(TpGeneralLocal):
    
        #@classmethod
        #def validate_object(cls,decw,object_id,download_path,connection_settings):
        #    entity_success,entity_messages = cls.validate_object_attrib(decw,object_id,download_path,connection_settings)
        #    payload_success,payload_messages = cls.validate_object_payload(decw,object_id,download_path,connection_settings)
        #    entity_messages:ObjectMessages = entity_messages
        #    all_messages:ObjectMessages = payload_messages
        #    all_messages.append(entity_messages)
        #    return entity_success and payload_success,all_messages
        
        #@classmethod
        #def validate_object_attrib(cls,decw,object_id,download_path,connection_settings):
        #    # Validate the local representation of an object
        #    messages = ObjectMessages("TpFile.Local.validate_object(for {object_id})")
        #    try:
        #        file_path_test = download_path+'/'+object_id+'/object.json'
        #        with open(file_path_test,'r') as f:
        #            obj_local = json.loads(f.read())
        #        valido_hasho = cls.compare_file_hash(file_path_test)
        #        if valido_hasho != True:
        #            messages.add_assert(False, "Encountered A bad hash object.json :"+file_path_test)
        #            return False,messages
        #    except:
        #        messages.add_assert(False==True, "Could not validate presense of file file:"+str(download_path+'/'+object_id+'/object.json'))
        #        return False,messages
        #    return len(messages.get_error_messages())== 0,messages   
        
        
        @classmethod
        def validate_object_payload(cls,decw,object_id,download_path,connection_settings):
            # Validate the Object
            result, messages = cls.validate_object_attrib(decw,object_id,download_path,connection_settings)
            if result == False:
                messages.add_assert(False, "Failed perliminary object validation validate_object_payload for TpFile.Local.validate_object_payload:")
                return False,messages

            # Validate the Payload
            messages = ObjectMessages("TpFile.Local.validate_object(for {object_id})")
            try:
                file_path = os.path.join(download_path,object_id, "payload.file")
                if not os.path.exists(file_path):
                    messages.add_assert(False==True, "Could not find local payload file")
                    return False,messages
                
                valido_hasheesh = cls.compare_file_hash(file_path, hash_func='sha2-256')
                if valido_hasheesh != True:
                    messages.add_assert(False, "Encountered A bad hash for payload.file:"+file_path)
            except:
                messages.add_assert(False, "Encountered an exception with the internal hash validation:"+tb.format_exc())
                
            return len(messages.get_error_messages())== 0,messages   
    #class LocalMirror():
