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
from .TpGeneralDecelium import TpGeneralDecelium
class TpFileDecelium(TpGeneralDecelium):
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

from .TpGeneralDeceliumMirror import TpGeneralDeceliumMirror
class TpFileDeceliumMirror(TpGeneralDeceliumMirror):
    pass

from .TpGeneralLocal import TpGeneralLocal
class TpFileLocal(TpGeneralLocal):
    pass
