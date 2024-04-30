class BaseData(dict):
    @staticmethod
    def get_keys(self):
        required = {'id': str, 'name': int}
        optional = {'age': int, 'interests': dict}
        return required, optional
    
    def do_validation(self,key,value):
        return value,""
    
    def __init__(self, init_dict):
        required_keys, optional_keys = self.get_keys()
        init_data = init_dict

        for key, expected_type in required_keys.items():
            if key not in init_dict:
                raise ValueError(f"Key '{key}' must be in the initialization dictionary")
            value = init_dict[key]
            if issubclass(expected_type, BaseData) and isinstance(value, dict):
                # Automatically instantiate BaseData subclasses
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
                if issubclass(expected_type, BaseData) and isinstance(value, dict):
                    value = expected_type(value)
                elif not isinstance(value, expected_type):
                    raise TypeError(f"Expected type {expected_type} for key '{key}', got {type(value)}")
                new_val,message=  self.do_validation(key,value)
                if new_val == None:
                    raise TypeError(f"do_validation did not pass for {expected_type} , {value}")                
                init_data[key] = value

        super().__init__(init_dict)
    def __setitem__(self, key, value):
        validated_value, message = self.do_validation(key, value)
        if validated_value is None:
            raise ValueError(f"Validation failed for key '{key}': {message}")
        super().__setitem__(key, validated_value)

    def set(self,key,val):
        self.__setitem__(key, val)
        return True
    
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
