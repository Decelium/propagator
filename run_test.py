import decelium_wallet.core as core
import os, sys,json
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

def new_corruption_config(setup_config:TestConfig,obj:dict,corruptions:list,evals:list,invalid_props:list):    
    corruptions = [CorruptionTestData.Instruction(corruption) for corruption in corruptions]
    evals = [CorruptionTestData.Eval(evaluation) for evaluation in evals]
    print("corruptions")
    print(json.dumps(corruptions,indent=3))
    return {'setup_config':setup_config,
                'obj':obj,
                'corruptions':corruptions,
                'corruption_evals':evals,                            
                'repair_target':'remote',
                'invalid_props':invalid_props,
                }
def test_corruptions():
    setup_config:TestConfig = test_setup()
    agent = SnapshotAgent()
    decw = setup_config.decw()
    obj = decw.net.download_entity({'self_id':setup_config.obj_id(),'attrib':True})
    configs = []
    
    # validate_entity - 
    validation_data = decw.net.validate_entity({'self_id':setup_config.obj_id()})
    # print(json.dumps(validation_data,indent=4))
    
    modes = ['remote_attrib','remote_payload','remote_mirror_attrib','remote_mirror_payload']
    for mode in modes:
        assert validation_data[mode][0][mode] == True
    corruption_suffix = {
                        'delete_payload':['payload'],
                        'corrupt_payload':['payload'],
                        'remove_attrib':['attrib'],
                        'rename_attrib_filename':['attrib'],
                        'corrupt_attrib':['attrib'],
                        'delete_entity':['attrib','payload'],}
    
    #for corrupt_remote in CorruptionTestData.Instruction.corruption_types:
    #    for corrupt_mirror in CorruptionTestData.Instruction.corruption_types:
    for corrupt_remote in ['delete_payload']: 
        for corrupt_mirror in  ['delete_payload']:
            assert type(corrupt_remote) == str
            assert type(corrupt_mirror) == str
            invalid_props = []
            invalid_props = invalid_props +  ['_'.join(['remote',suffix]) for suffix in  corruption_suffix[corrupt_remote]]
            invalid_props = invalid_props + ['_'.join(['remote_mirror',suffix]) for suffix in  corruption_suffix[corrupt_mirror]]

            configs.append(new_corruption_config(setup_config,obj,
                [{'corruption':corrupt_remote,"mode":'remote'},
                 {'corruption':corrupt_mirror,"mode":'remote_mirror'},],
                [{'target':'local','status':['complete']},
                 {'target':'remote','status':['object_missing','payload_missing']},
                {'target':'remote_mirror','status':['object_missing','payload_missing']}],
                invalid_props))


    print("corruption tests :"+str(len(configs)))
    for corruption_config in configs:
        print("Testing: \n"+ json.dumps(corruption_config['corruptions'],indent=4))
        agent.run_corruption_test(corruption_config)

test_corruptions()