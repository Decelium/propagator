from functools import wraps
from types import FunctionType
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
    
    def __init__(self, init_dict):
        if isinstance(init_dict,type(self)):
            super().__init__(init_dict)      
            return
        required_keys, optional_keys = self.get_keys()
        init_data = init_dict
        
        for key, expected_type in required_keys.items():
            if key not in init_dict:
                raise ValueError(f"Key '{key}' must be in the initialization dictionary")
            value = init_dict[key]
            if isinstance(expected_type, FunctionType):
                value = expected_type(value)
            elif issubclass(expected_type, BaseData) and isinstance(value, dict):
                value = expected_type(value)
            elif not isinstance(value, expected_type):
                raise TypeError(f"Expected type {expected_type} for key '{key}', got {type(value)}")
            new_val,message=  self.do_validation(key,value)
            if new_val == None:
                raise TypeError(f"do_validation did not pass for {expected_type} , {value}")
            init_data[key] = new_val

        for key, expected_type in optional_keys.items():
            if key in init_dict:
                value = init_dict[key]
                if isinstance(expected_type, FunctionType):
                    value = expected_type(value)
                elif issubclass(expected_type, BaseData) and isinstance(value, dict):
                    value = expected_type(value)
                elif not isinstance(value, expected_type):
                    raise TypeError(f"Expected type {expected_type} for key '{key}', got {type(value)}")
                new_val,message=  self.do_validation(key,value)
                if new_val == None:
                    raise TypeError(f"do_validation did not pass for {expected_type} , {value}")                
                init_data[key] = value

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
    '''
    def __getattr__(self, name):
        if name.startswith("set_"):
            attr_name = name[4:]  
            return lambda: self.set 
        if name.startswith("get_"):
            attr_name = name[4:]  
            if attr_name in self:
                return lambda: self[attr_name]  
            else:
                raise AttributeError(f"No such attribute: {attr_name}")
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")
    '''

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
            print ("Validating "+ key)
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

from decelium_wallet import core as core

class ConnectionConfig(BaseData):
    def decw(self) -> core:
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
        required = {'decw':core,
                    'user_context':dict,
                    'connection_settings':dict,
                    'backup_path':str,
                    'local_test_folder':str,
                    }
        return required,{}


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