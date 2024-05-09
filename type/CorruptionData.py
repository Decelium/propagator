try:
    from BaseData import BaseData
except:
    from .BaseData import BaseData
'''

    {'target':'local','status':['complete']},
    {'target':'remote','status':['object_missing','payload_missing']},
    {'target':'remote_mirror','status':['object_missing','payload_missing']},

'''
class CorruptionTestData(BaseData):
    class Instruction(BaseData):
        corruption_types = ['delete_payload','corrupt_payload','remove_attrib','corrupt_attrib','rename_attrib_filename']
        mode_types = ['remote', 'local', 'remote_mirror']
        def get_keys(self):
            required = {'corruption':lambda v: v if v in self.corruption_types else self.do_raise("corruption"),
                        'mode':lambda v: v if v in self.mode_types else self.do_raise("mode"),
                        }
            return required,{}
        
    class Eval(BaseData):
        target_types = ['local','remote','remote_mirror']
        status_contains = ['complete', 'object_missing', 'payload_missing']
        def get_keys(self):
            required = {'target':lambda v: v if v in self.target_types else self.do_raise("target"),
                        'status':lambda value: value if set(value).issubset(set(self.status_contains)) else self.do_raise("status"),
                        }
            return required,{}        
    #class Validation(BaseData):
    #    def get_keys(self):
    #        required = {'corruption':str, 
    #                    "mode":str}
    def get_keys(self):
        required = {
                    'pre_evaluation':list,
                    'corruptions':list, 
                    'post_evaluation':list,
                    'repair_actions':list
                    }
