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
from .DsGeneral import TpFacade
from .DsGeneralDecelium import DsGeneralDecelium
from .DsGeneralDeceliumMirror import DsGeneralDeceliumMirror
from .DsGeneralLocal import DsGeneralLocal

class TpAttrib(TpFacade):
    class Decelium(DsGeneralDecelium):
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
            messages = ObjectMessages("DsFileDecelium.validate_object_entity_mirror(for {"+object_id+"})")
            obj_valid = decw.net.validate_entity_hash( {'api_key':'UNDEFINED', 'self_id':object_id})
            if messages.add_assert(obj_valid == True, f"validate_entity_hash({object_id}) seems to be invalid, as reported by DB validate_object_entity_mirror:"+str(obj_valid)) == False:
                return False, messages
            return len(messages.get_error_messages()) == 0, messages      

        @classmethod
        def validate_object_payload(cls,decw,object_id,download_path,connection_settings,obj_remote = None):
            messages = ObjectMessages("TpAttrib.Decelium.validate_object_payload(for {"+object_id+"})")
            return None, messages  

    class DeceliumMirror(DsGeneralDeceliumMirror):
        @classmethod
        def validate_object(cls,decw,object_id,download_path,connection_settings,obj_remote = None):
            entity_success,entity_messages = cls.validate_object_attrib(decw,object_id,download_path,connection_settings)
            return entity_success,entity_messages

        @classmethod
        def validate_object_attrib(cls,decw,object_id,download_path,connection_settings,obj_remote = None):
            messages = ObjectMessages("DsIPFSDeceliumMirror.validate_object_entity_mirror(for {"+object_id+"})")
            obj_valid = decw.net.validate_entity_hash( {'api_key':'UNDEFINED', 'self_id':object_id,'mirror':True})
            if messages.add_assert(obj_valid == True, f"validate_entity_hash({object_id}) seems to be invalid, as reported by DB validate_object_entity_mirror:"+str(obj_valid)) == False:
                return False, messages
            return len(messages.get_error_messages()) == 0, messages      

        @classmethod
        def validate_object_payload(cls,decw,object_id,download_path,connection_settings,obj_remote = None):
            messages = ObjectMessages("TpAttrib.DeceliumMirror.validate_object_entity_payload(for {"+object_id+"})")
            return None,messages

    class Local(DsGeneralLocal): 
        @classmethod
        def push_payload_to(cls,ds_remote,decw,obj,download_path,connection_settings):
            messages = ObjectMessages("DsFileLocal(for IPFS).push_payload_to_remote")

            return None, messages        
        @classmethod
        def validate_object(cls,decw,object_id,download_path,connection_settings):
            entity_success,entity_messages = cls.validate_object_attrib(decw,object_id,download_path,connection_settings)
            return entity_success,entity_messages        

        
        @classmethod
        def validate_object_payload(cls,decw,object_id,download_path,connection_settings):
            # Validate the Object
            result, messages = cls.validate_object_attrib(decw,object_id,download_path,connection_settings)
            if result == False:
                messages.add_assert(False, "Failed perliminary object validation validate_object_payload for TpFile.Local.validate_object_payload:")
                return False,messages
            return None,messages
        
    class LocalFilesystem(Local):
        pass
