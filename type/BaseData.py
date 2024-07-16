from functools import wraps
from types import FunctionType
import json
def auto_c(parameter_type):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            new_args = [
                parameter_type(arg) if isinstance(arg, dict) and parameter_type is not dict else arg
                for arg in args
            ]
            new_kwargs = {
                k: parameter_type(v) if isinstance(v, dict) and parameter_type is not dict else v
                for k, v in kwargs.items()
            }
            return func(*new_args, **new_kwargs)
        return wrapper
    return decorator
# TODO - Remove duplicate code

class BaseData(dict):
    def do_raise(self,description=None):
        if description == None:
            description = "Invalid corruption type"
        raise ValueError(description)    
    
    def get_keys(self):
        required = {'id': str, 'name': int}
        optional = {'age': int, 'interests': dict}
        return required, optional
    
    def do_validation(self,key,value):
        return value,""

    def _validate_and_convert(self, key, expected_type, init_dict):
        value = init_dict[key]
        """Validates and converts a single key value based on its expected type."""
        if isinstance(expected_type, FunctionType):
            value = expected_type(value)
        elif issubclass(expected_type, BaseData) and isinstance(value, dict):
            value = expected_type(value)
        elif not isinstance(value, expected_type):
            raise TypeError(f"Expected type {expected_type} for key '{key}', got {type(value)}")

        new_val, message = self.do_validation(key, value)
        if new_val is None:
            raise TypeError(f"do_validation did not pass for {expected_type} , {value}")

        init_dict[key] = new_val
    
    def do_pre_process(self,in_dict):
        return in_dict
    
    def __init__(self, in_dict,trim=False):
        if isinstance(in_dict,type(self)):
            super().__init__(in_dict)      
            return
        in_dict = self.do_pre_process(in_dict)
        required_keys, optional_keys = self.get_keys()
        
        if trim == False:
            init_dict = in_dict
        else:
            all_keys = list(required_keys.keys()) + list(optional_keys.keys())
            init_dict = in_dict.copy()
            for k in init_dict.keys():
                if k not in all_keys:
                    del(init_dict[k])
                
        # Validate and convert required keys
        for key, expected_type in required_keys.items():
            if key not in init_dict:
                raise ValueError(f"Key '{key}' must be in the initialization dictionary")
            self._validate_and_convert(key, expected_type, init_dict)

        # Validate and convert optional keys if they are present
        for key, expected_type in optional_keys.items():
            if key in init_dict:
                self._validate_and_convert(key, expected_type, init_dict)        
        super().__init__(init_dict)
        
    def __setitem__(self, key, value):
        # TODO, Generalize the validation check, and run it on set in a complete manner
        validated_value, message = self.do_validation(key, value)
        if validated_value is None:
            raise ValueError(f"Validation failed for key '{key}': {message}")
        super().__setitem__(key, validated_value)

    def set(self,key,val):
        self.__setitem__(key, val)
        return True


def run_simple_test():

    class BodyPartData(BaseData):
        def get_keys(self):
            required = {'id': str, 'part_name': str}
            optional = {'health': int, 'skills': dict}
            return required, optional    

    class HumanData(BaseData):
        def get_keys(self):
            required = {'id': str, 'name': str}
            optional = {'age': int, 'interests': dict,'arm':BodyPartData}
            return required, optional    
        
        def do_validation(self,key,value):
            if key == 'age':
                assert value > 0 and value < 120, "Humans must have a valid age range"
            return value,""

    class CarData(BaseData):
        def get_keys():
            required = {'id': str, 'name': str}
            optional = {'driver': HumanData}
            return required, optional    
    import pprint    
    # Example usage
    humanData = HumanData({'id': '123', 
                    'name': "Jeff", 
                    'age': 30,
                    'arm':{'id': "bigArm", 'part_name': "part_1"}})


    arm = BodyPartData({'id': "bigArm", 'part_name': "part_1"})
    humanData2 = HumanData({'id': '123', 
                    'name': "Jeff", 
                    'age': 30,
                    'arm':arm})

#from decelium_wallet import core as core



class ConnectionConfig(BaseData):
    def decw(self):
        return self['decw']
    def user_context(self) -> str:
        return self['user_context']
    def connection_settings(self) -> dict:
        return self['connection_settings']
    def backup_path(self) -> str:
        return self['backup_path']
    def local_test_folder(self) -> str:
        return self['local_test_folder']
    
    def get_keys(self):
        required = {'decw':lambda v:v, # (╯°□°)╯︵ ┻━┻ 
                    'user_context':dict,
                    'connection_settings':dict,
                    'backup_path':str,
                    'local_test_folder':str,
                    }
        return required,{}

# lambda v: v if (v in self.corruption_types and type(v) is str) else self.do_raise("corruption"),
class TestConfig(ConnectionConfig):
    def decelium_path(self) ->str:
        return self['decelium_path']
    def obj_id(self) -> str:
        return self['obj_id']
    def eval_context(self) -> dict:
        return self['eval_context']
    
    def get_keys(self):
        super_required,optional = super().get_keys()
        required = {** super_required,
                    'decelium_path':str,
                    'obj_id':str,
                    'eval_context':dict,
                    }
        return required,optional

class DeploymentConfig(BaseData):
    def get_keys(self):
        required = {'server_address_config':ServerAddressConfig,
                    'branch':str
                    }
        optional = {}
        return required,optional

class ServerAddressConfig(BaseData):
    def node_url(self):
        return f"{self['node_protocol']}://{self['host']}:{self['node_port']}/{self['node_url']}"
    
    def base_url(self):
        return f"{self['node_protocol']}://{self['host']}"
    
    def web_url(self):
        return f"{self['node_protocol']}://{self['host']}:{self['node_port']}/"
    
    def ipfs_connection_settings(self):
        return {
            'host':self['host'],
            'port':self['ipfs_port'],
            'protocol':self['ipfs_protocol'],
        }
    
    def get_host(self):
        return self['host']
    def get_port(self):
        return self['port']
    
    def get_protocol(self):
        return self['node_protocol']
    
    def get_keys(self):
        required = {
            'host':str,
            'node_port':str,
            'node_protocol':str,
            'node_url':str,
            'ipfs_port':str,
            'ipfs_protocol':str            
        }
        return required,{}

    def do_pre_process(self,in_dict):
        defaults = {
            'node_port':'5000',
            'node_protocol':'http',
            'node_url':'/data/query',
            'ipfs_port':'5001',
            'ipfs_protocol':'http'
        }
        for k in defaults:
            if not k in in_dict:
                in_dict[k] = defaults[k]
        return in_dict




class CommandResponse(BaseData):
    def debug_print(self):
        print(json.dumps({
            'status':self['status'],
            'response':self['response'],
                          },indent=1))
        print("STOUT")
        print(self['stout'])
        print("STERR")
        print(self['sterr'])

    def get_keys(self):
        required = {
                    'status':float, 
                    'response':dict, 
                    'stout':str,
                    'sterr':str
                    }
        return required,{}
    

class AgentCommand(BaseData):
    def __init__(self,in_dict,trim=False):
        propagator_commands = ["validate","backup","append","status","purge_corrupt","push","repair","pull"]
        
        self.legal_commands = [
                         'gitpull','gitpush', # GIT (For Servers)
                         'test','redeploy','undeploy', # SERVER OPS (For servers)
                         'deploy_app', # APP OPS (For apps)
                         ]
        for pcommand in propagator_commands:
            self.legal_commands.append('p'+pcommand)
        super().__init__(in_dict,trim=False)
       
    def get_keys(self):
        required = {
                    'command':lambda v: v if (v in self.legal_commands and type(v) is str) else self.do_raise("Could not identify a valid commad type"),
                    }
        optional = {
                    'src_branch':str, 
                    'dst_branch':str, 
                    'src_server':str, 
                    'dst_server':str, 
                    'bck_id':str, 
                    }
        
        return required,optional