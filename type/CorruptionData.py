try:
    from BaseData import BaseData, ConnectionConfig, TestConfig
except:
    from .BaseData import BaseData, ConnectionConfig, TestConfig
import json
class ValidationStatusResult():
        # TODO Define ValidationStatusResult
        def get_keys(self):
            required = {'corruption':lambda v: v if (v in self.corruption_types and type(v) is str) else self.do_raise("corruption"),
                        'mode':lambda v: v if v in self.mode_types else self.do_raise("mode"),
                        }
            return required,{}


class CorruptionTestData(BaseData):
    class Instruction(BaseData):
        corruption_types = ['delete_payload','corrupt_payload','remove_attrib','rename_attrib_filename','corrupt_attrib','delete_entity']
        mode_types = ['remote', 'local', 'remote_mirror']
        def get_keys(self):
            required = {'corruption':lambda v: v if (v in self.corruption_types and type(v) is str) else self.do_raise("corruption"),
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
            return required,{'invalid_props':list,}   ## TODO, harden   invalid_props concepts    
        
    def get_keys(self):
        required = {
                    'pre_evaluation':list,
                    'corruptions':list, 
                    'post_evaluation':list,
                    'repair_actions':list
                    }
    @classmethod
    def new_corruption_config(cls,
                              setup_config:TestConfig,
                              obj:dict,corruptions:list,pre_evals:list,
                              invalid_props:list,final_evals,
                              do_repair:bool,
                              post_repair_status:bool,push_target:str):    
        corruptions = [CorruptionTestData.Instruction(corruption) for corruption in corruptions]
        pre_evals = [CorruptionTestData.Eval(evaluation) for evaluation in pre_evals]
        final_evals = [CorruptionTestData.Eval(evaluation) for evaluation in final_evals]
        print("corruptions")
        print(json.dumps(corruptions,indent=3))
        dic= {'setup_config':setup_config,
                'obj':obj,
                'corruptions':corruptions,
                'corruption_evals':pre_evals,
                'invalid_props':invalid_props,
                'do_repair':do_repair,
                'post_repair_status':post_repair_status,
                'final_evals':final_evals,
                'push_target':push_target,
                    }
        return dic
    


    @staticmethod
    def new_repair_corruption_config(corruption_1,
                                    corruption_2,
                                    setup_config,
                                    obj,
                                    c_target_1,
                                    c_target_2,
                                    c_target_reserve,
                                    do_repair,
                                    push_target,
                                    target_type
                                    ):

            corruption_suffix_full = {
                                'delete_payload':['payload'],
                                'corrupt_payload':['payload'],
                                'remove_attrib':['attrib'], 
                                'rename_attrib_filename':['attrib'],
                                'corrupt_attrib':['attrib'], 
                                'delete_entity':['attrib','payload']
                                }  

            corruption_suffix_attrib_only = {
                                'delete_payload':[],
                                'corrupt_payload':[],
                                'remove_attrib':['attrib'], 
                                'rename_attrib_filename':['attrib'],
                                'corrupt_attrib':['attrib'], 
                                'delete_entity':['attrib']
                                }    
                      
            # The corruption Test -----------------------
            assert type(corruption_1) == str # The first corruption to apply to c_target_1
            assert type(corruption_2) == str # The second corruption to apply to c_target_2
            assert c_target_1 in ['remote','local','remote_mirror'] # The first datasource to corrupt
            assert c_target_2 in ['remote','local','remote_mirror'] # The second datasource to corrupt
            assert c_target_reserve in ['remote_mirror','local'] # The datasource that will be held stable, so we can restore after
            assert do_repair in [True,False] # Are we testing the repair process? (Only relevant for remote and remote_mirror tests)
            assert push_target in ['local','remote'] # Where we would like to push the repair data
            attrib_only_targets = ['host','dict','directory','node']
            corruption_map = corruption_suffix_full
            if (target_type in attrib_only_targets):
                corruption_map = corruption_suffix_attrib_only
            invalid_props = []
            pre_invalid_props = []
            post_invalid_props = []
            # The corruptions we can apply, and what they will break.
            #
            if 'attrib' in corruption_map[corruption_2] and 'attrib' in corruption_map[corruption_1]:
                # If both attributes are corrupt, then no repair can validate the payload
                post_invalid_props = [f'{c_target_1}_payload',f'{c_target_2}_payload']
            post_invalid_remote =  ['_'.join([c_target_1,suffix]) for suffix in corruption_map[corruption_1] if suffix in corruption_map[corruption_2] ]
            post_invalid_remote_mirror =  ['_'.join([c_target_2,suffix]) for suffix in  corruption_map[corruption_2] if suffix in corruption_map[corruption_1]]
            post_invalid_props = post_invalid_props+ post_invalid_remote + post_invalid_remote_mirror

            #
            if 'attrib' in corruption_map[corruption_2]:
                pre_invalid_props =  pre_invalid_props + [f'{c_target_2}_payload']
            if 'attrib' in corruption_map[corruption_1]:
                pre_invalid_props =  pre_invalid_props + [f'{c_target_1}_payload']
            pre_invalid_remote =  ['_'.join([c_target_1,suffix]) for suffix in corruption_map[corruption_1] ]
            pre_invalid_remote_mirror =  ['_'.join([c_target_2,suffix]) for suffix in  corruption_map[corruption_2]]
            pre_invalid_props = pre_invalid_props+ pre_invalid_remote + pre_invalid_remote_mirror

            #
            if do_repair == True:
                invalid_props  = post_invalid_props
            else:
                invalid_props = pre_invalid_props
            print("\n\nINVALID DEBUG IN new_repair_corruption_config")
            print("-- do_repair",do_repair)
            print("-- pre_invalid_remote",pre_invalid_remote)
            print("-- pre_invalid_remote_mirror",pre_invalid_remote_mirror)
            print("-- pre_invalid_props",pre_invalid_props)
            print("\n\n")
            repair_success_expectation = True
            
            if 'payload' in corruption_map[corruption_1] and 'payload' in corruption_map[corruption_2]:
                repair_success_expectation = False
            elif 'attrib' in corruption_map[corruption_1] and 'attrib' in corruption_map[corruption_2]:
                repair_success_expectation = False
            pre_eval_1 = {'target':c_target_1,'status':['object_missing','payload_missing']}
            pre_eval_2 = {'target':c_target_2,'status':['object_missing','payload_missing']}

            # Small patch to acknolwedge that payload corruption of attribute only entities should not have any effect
            if len(pre_invalid_remote) == 0:
                pre_eval_1 = {'target':c_target_1,'status':['complete']} # The corruption should not do anything
                if do_repair == False:
                    assert corruption_1 in ['delete_payload','corrupt_payload']
                assert target_type in attrib_only_targets

            if len(pre_invalid_remote_mirror) == 0:
                pre_eval_2 = {'target':c_target_2,'status':['complete']} # The corruption should not do anything
                if do_repair == False:
                    assert corruption_2 in ['delete_payload','corrupt_payload']
                assert target_type in attrib_only_targets

            pre_evals = [
                {'target':c_target_reserve,'status':['complete']},
                pre_eval_1,
                pre_eval_2
                ]
            final_evals = []

            if repair_success_expectation == True and do_repair == True:
                final_evals = [
                    {'target':c_target_reserve,'status':['complete']},
                    {'target':c_target_1,'status':['complete']},
                    {'target':c_target_2,'status':['complete']}
                    ]
                invalid_props = []
            
            ## TODO refactor for repair. Kind of janky
            
            config = CorruptionTestData.new_corruption_config(setup_config,obj,
                [{'corruption':corruption_1,"mode":c_target_1},
                    {'corruption':corruption_2,"mode":c_target_2},],
                pre_evals,
                invalid_props,
                final_evals,
                do_repair,
                repair_success_expectation,
                push_target)
            return config

