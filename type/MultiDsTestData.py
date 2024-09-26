from .BaseData import BaseData

class MultiDsTestData(BaseData):
    #corruption_types = ['delete_payload','corrupt_payload','remove_attrib','rename_attrib_filename','corrupt_attrib','delete_entity']
    #mode_types = ['remote', 'local', 'remote_mirror']
    def get_keys(self):
        required = {'corruption':lambda v: v if (v in self.corruption_types and type(v) is str) else self.do_raise("corruption"),
                    'mode':lambda v: v if v in self.mode_types else self.do_raise("mode"),
                    }
        required = {'corruption':lambda v: v if (v in self.corruption_types and type(v) is str) else self.do_raise("corruption"),
                    'mode':lambda v: v if v in self.mode_types else self.do_raise("mode"),
                    }
        
        return required,{}