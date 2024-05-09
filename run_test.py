import decelium_wallet.core as core
import os, sys
import shutil
sys.path.append('..')
#from type.BaseData import BaseData,auto_c
from type.CorruptionData import CorruptionTestData
#from actions import CreateDecw
from type.BaseData import TestConfig
from actions.SnapshotAgent import SnapshotAgent
def test_setup() -> TestConfig:
    # setup connection 
    print("---- 1: Doing Setup")
    agent = SnapshotAgent()

    decw, connected = agent.create_wallet_action({
         'wallet_path': '../.wallet.dec',
         'wallet_password_path':'../.wallet.dec.password',
         'fabric_url': 'http://devdecelium.com:5000/data/query',
        })
    
    user_context = {
            'api_key':decw.dw.pubk()
    }
    connection_settings = {'host': "devdecelium.com",
                            'port':5001,
                            'protocol':"http"
    }
    ipfs_req_context = {**user_context, **{
            'file_type':'ipfs', 
            'connection_settings':connection_settings
    }}
    decelium_path = 'temp/test_folder.ipfs'
    local_test_folder = './test/testdata/test_folder'
    # --- Remove old snapshot #
    backup_path = "./test/system_backup_test"
    try:
        shutil.rmtree(backup_path)
    except:
        pass

    print("---- 2: Doing Small Upload")
    obj_id = agent.upload_directory_to_remote({
        'local_path': local_test_folder,
        'decelium_path': decelium_path,
        'decw': decw,
        'ipfs_req_context': ipfs_req_context,
        'user_context': user_context
    })
    eval_context = {
        'backup_path':backup_path,
        'self_id':obj_id,
        'connection_settings':connection_settings,
        'decw':decw}

    agent.evaluate_object_status({**eval_context,'target':'local','status':['object_missing','payload_missing']})
    agent.evaluate_object_status({**eval_context,'target':'remote','status':['complete']})
    agent.evaluate_object_status({**eval_context,'target':'remote_mirror','status':['complete']})

    print("---- 2: Doing Small Pull")
    obj,new_cids = agent.append_object_from_remote({
     'decw':decw,
     'obj_id':obj_id,
     'connection_settings':connection_settings,
     'backup_path':backup_path,
    })

    agent.evaluate_object_status({**eval_context,'target':'local','status':['complete']})
    agent.evaluate_object_status({**eval_context,'target':'remote','status':['complete']})
    agent.evaluate_object_status({**eval_context,'target':'remote_mirror','status':['complete']})

    return TestConfig({'decw':decw,
            'user_context':user_context,
            'connection_settings':connection_settings,
            'decelium_path':decelium_path,
            'eval_context':eval_context,
            'obj_id':obj_id,
            'backup_path':backup_path,
            })

def perliminary_tests():
    agent = SnapshotAgent()
    setup_config = test_setup()
    print("---- 3: Doing Small Delete")
    agent.delete_object_from_remote({
        'decw':setup_config.decw(),
        'user_context':setup_config.user_context(),
        'connection_settings':setup_config.connection_settings(),
        'path': setup_config.decelium_path(),     
    })
    agent.evaluate_object_status({**setup_config.eval_context(),'target':'local','status':['complete']})
    agent.evaluate_object_status({**setup_config.eval_context(),'target':'remote','status':['object_missing','payload_missing']})   
    agent.evaluate_object_status({**setup_config.eval_context(),'target':'remote_mirror','status':['object_missing','payload_missing']})
    print("---- 3: Doing Small Push")
    agent.push_from_snapshot_to_remote({
        'decw': setup_config.decw(),
        'obj_id':setup_config.obj_id(),
        'user_context':setup_config.user_context(),
        'connection_settings':setup_config.connection_settings(),
        'backup_path':setup_config.backup_path(),
    })
    agent.evaluate_object_status({**setup_config.eval_context(),'target':'local','status':['complete']})
    agent.evaluate_object_status({**setup_config.eval_context(),'target':'remote','status':['complete']})  
    agent.evaluate_object_status({**setup_config.eval_context(),'target':'remote_mirror','status':['complete']})

def new_corruption_config(setup_config:TestConfig,obj:dict,corruptions:list,evals:list):    
    corruptions = [CorruptionTestData.Instruction(corruption) for corruption in corruptions]
    evals = [CorruptionTestData.Eval(evaluation) for evaluation in evals]

    return {'setup_config':setup_config,
                'obj':obj,
                'corruptions':corruptions,
                'corruption_evals':evals,                            
                'repair_target':'remote',
                }
def test_corruptions():
    setup_config:TestConfig = test_setup()
    agent = SnapshotAgent()
    decw = setup_config.decw()
    obj = decw.net.download_entity({'self_id':setup_config.obj_id(),'attrib':True})
    
    configs = []
    configs.append(new_corruption_config(setup_config,obj,
        [{'corruption':"delete_payload","mode":'remote'},                             
        {'corruption':"delete_payload","mode":'remote_mirror'},],
        [{'target':'local','status':['complete']},
        {'target':'remote','status':['object_missing','payload_missing']},
        {'target':'remote_mirror','status':['object_missing','payload_missing']}]))

    for corruption_config in configs:
        agent.run_corruption_test(corruption_config)

# test_simple_snapshot()
# test_setup()
# perliminary_tests()
test_corruptions()
#from propagator.datasource.TpIPFSDecelium import TpIPFSDecelium
#from propagator.datasource.TpIPFSLocal import TpIPFSLocal
#from propagator.Snapshot import Snapshot

'''
def run_corruption(decw,
                         obj,
                         backup_path,
                         connection_settings,
                         eval_context,
                         user_context,
                         corruption):
    corrupt_object_backup = CorruptObject()
    change_remote_object_name = ChangeRemoteObjectName()
    pull_object_from_remote = PullObjectFromRemote()
    push_from_snapshot_to_remote = PushFromSnapshotToRemote()

    corruption = CorruptionTestData.Instruction(corruption)
    backup_instruction  ={
        'decw': decw,
        'obj_id':obj['self_id'],
        'backup_path':backup_path,        
        'connection_settings':connection_settings,        
    }
    backup_instruction["corruption"] = corruption['corruption']
    backup_instruction["pre_status"] = corruption['pre_status']
    backup_instruction["post_status"] = corruption['post_status']
    backup_instruction["mode"] = corruption['mode']
    backup_instruction.update(corruption)
    corrupt_object_backup(backup_instruction)

    if corruption['mode'] == 'local':
        merge_function = pull_object_from_remote
        truth_target = 'remote'
        corruption_target = 'local'
    elif corruption['mode'] == 'remote':
        merge_function = push_from_snapshot_to_remote
        truth_target = 'local'
        corruption_target = 'remote'
    else:
        assert True==False, "Forcing a failure as we are not corrupting remote or local"

    # 2 - 
    evaluate_object_status({**eval_context,'target':truth_target,'status':['complete']}) 
    evaluate_object_status({**eval_context,'target':corruption_target,'status':[backup_instruction["pre_status"]]})
    merge_function({
        'connection_settings':connection_settings,
        'backup_path':backup_path,
        'overwrite': False,
        'decw': decw,
        'user_context': user_context,
        'obj_id':obj['self_id'],
        'expected_result':True,
    })
    evaluate_object_status({**eval_context,'target':truth_target,'status':['complete']})  
    evaluate_object_status({**eval_context,'target':corruption_target,'status':[backup_instruction["post_status"]]})

def run_corruption_test(setup_config:TestConfig,
                         obj:dict,
                         all_corruptions:dict):

    for corruption_list in all_corruptions:
        for corruption in corruption_list:
            run_corruption(setup_config.decw(),
                                    obj,
                                    setup_config.backup_path(),
                                    setup_config.connection_settings(),
                                    setup_config.eval_context(),
                                    setup_config.user_context(),
                                    corruption)

'''


'''
        #[{'corruption':"delete_payload","post_status":'complete', "mode":'local','pre_status':'payload_missing'}], 
        #[{'corruption':"corrupt_payload","post_status":'complete', "mode":'local','pre_status':'payload_missing'}], 
        #[{'corruption':"remove_attrib","post_status":'complete', "mode":'local','pre_status':'payload_missing'}], 
        #[{'corruption':"corrupt_attrib","post_status":'complete', "mode":'local','pre_status':'payload_missing'}], 
        #[{'corruption':"rename_attrib_filename","post_status":'complete', "mode":'local','pre_status':'payload_missing'}],
        #[{'corruption':"corrupt_payload","post_status":'complete', "mode":'remote','pre_status':'payload_missing'}], 
        #[{'corruption':"remove_attrib","post_status":'complete', "mode":'remote','pre_status':'payload_missing'}],
        #[{'corruption':"rename_attrib_filename","post_status":'complete', "mode":'remote','pre_status':'payload_missing'}],
'''
