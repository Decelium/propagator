try:
    from BaseData import BaseData
except:
    from .BaseData import BaseData

class CorruptionTestData(BaseData):
    class Instruction(BaseData):
        corruption_types = ['delete_payload','corrupt_payload','remove_attrib','corrupt_attrib','rename_attrib_filename']
        mode_types = ['remote', 'local', 'mirror']
        def get_keys(self):
            required = {'corruption':lambda v: v if v in self.corruption_types else self.do_raise("corruption"),
                        'mode':lambda v: v if v in self.mode_types else self.do_raise("mode"),
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
