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

class TpAttrib(TpFacade):
    class Decelium(TpGeneralDecelium):
        @classmethod 
        def reupload_payload(cls,decw,obj):
            messages = ObjectMessages("TpAttrib.Decelium.reupload_payload_stub")
            # I think we have nothing to do, as no payload really exists
            return None, messages
        
        @classmethod
        def validate_object(cls,decw,object_id,download_path,connection_settings,obj_remote = None):
            entity_success,entity_messages = cls.validate_object_attrib(decw,object_id,download_path,connection_settings)
            return entity_success,entity_messages

        @classmethod
        def validate_object_attrib(cls,decw,object_id,download_path,connection_settings,obj_remote = None):
            messages = ObjectMessages("TpFileDecelium.validate_object_entity_mirror(for {"+object_id+"})")
            obj_valid = decw.net.validate_entity_hash( {'api_key':'UNDEFINED', 'self_id':object_id})
            if messages.add_assert(obj_valid == True, f"validate_entity_hash({object_id}) seems to be invalid, as reported by DB validate_object_entity_mirror:"+str(obj_valid)) == False:
                return False, messages
            return len(messages.get_error_messages()) == 0, messages      

        @classmethod
        def validate_object_payload(cls,decw,object_id,download_path,connection_settings,obj_remote = None):
            messages = ObjectMessages("TpAttrib.Decelium.validate_object_payload(for {"+object_id+"})")
            return None, messages  

    class DeceliumMirror(TpGeneralDeceliumMirror):
        @classmethod
        def validate_object(cls,decw,object_id,download_path,connection_settings,obj_remote = None):
            entity_success,entity_messages = cls.validate_object_attrib(decw,object_id,download_path,connection_settings)
            #payload_success,payload_messages = cls.validate_object_payload(decw,object_id,download_path,connection_settings)
            #entity_messages:ObjectMessages = entity_messages
            #all_messages:ObjectMessages = payload_messages
            #all_messages.append(entity_messages)
            return entity_success,entity_messages

        @classmethod
        def validate_object_attrib(cls,decw,object_id,download_path,connection_settings,obj_remote = None):
            messages = ObjectMessages("TpIPFSDeceliumMirror.validate_object_entity_mirror(for {"+object_id+"})")
            obj_valid = decw.net.validate_entity_hash( {'api_key':'UNDEFINED', 'self_id':object_id,'mirror':True})
            if messages.add_assert(obj_valid == True, f"validate_entity_hash({object_id}) seems to be invalid, as reported by DB validate_object_entity_mirror:"+str(obj_valid)) == False:
                return False, messages
            return len(messages.get_error_messages()) == 0, messages      

        @classmethod
        def validate_object_payload(cls,decw,object_id,download_path,connection_settings,obj_remote = None):
            messages = ObjectMessages("TpAttrib.DeceliumMirror.validate_object_entity_payload(for {"+object_id+"})")
            '''
            obj_valid = decw.net.validate_payload( {'api_key':'UNDEFINED', 'self_id':object_id,'mirror':True})
            if messages.add_assert(obj_valid == True, f"{object_id} seems to be invalid, as reported by DB validate_object_payload_mirror:"+str(obj_valid)) == False:
                return False, messages
            return len(messages.get_error_messages()) == 0, messages    
            '''
            return None,messages

    class Local(TpGeneralLocal): 
        @classmethod
        def push_payload_to(cls,ds_remote,decw,obj,download_path,connection_settings):
            messages = ObjectMessages("TpFileLocal(for IPFS).push_payload_to_remote")
            '''
            # For this kind of file, no additional payload handshaking is required. Create handles payload managment via attrib
            try:
                file_path = os.path.join(download_path,obj['self_id'], "payload.file")
                if not os.path.exists(file_path):
                    messages.add_assert(False==True, "Could not find local payload file")
                    return False,messages

                
                
                valido_hasheesh = cls.compare_file_hash(file_path, hash_func='sha2-256')
                if valido_hasheesh != True:
                    messages.add_assert(False, "Encountered A bad hash for payload.file:"+file_path)
                with open(file_path,'r') as f:
                    payload = f.read()
                
                success, messages = ds_remote.reupload_payload(decw,{'self_id':obj['self_id'],'payload':payload})
                assert type(success) == bool
                assert type(messages) == ObjectMessages
                return success, messages
                
            except:
                messages.add_assert(False, "Encountered an exception with the internal hash validation:"+tb.format_exc())

            '''
            return None, messages        
        @classmethod
        def validate_object(cls,decw,object_id,download_path,connection_settings):
            entity_success,entity_messages = cls.validate_object_attrib(decw,object_id,download_path,connection_settings)
            #payload_success,payload_messages = cls.validate_object_payload(decw,object_id,download_path,connection_settings)
            #entity_messages:ObjectMessages = entity_messages
            #all_messages:ObjectMessages = payload_messages
            #all_messages.append(entity_messages)
            return entity_success,entity_messages        

        
        @classmethod
        def validate_object_payload(cls,decw,object_id,download_path,connection_settings):
            # Validate the Object
            result, messages = cls.validate_object_attrib(decw,object_id,download_path,connection_settings)
            if result == False:
                messages.add_assert(False, "Failed perliminary object validation validate_object_payload for TpFile.Local.validate_object_payload:")
                return False,messages
            return None,messages
            '''
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
            '''
    #class LocalMirror():