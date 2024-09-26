
import traceback as tb
try:
    from ..Snapshot import Snapshot
    from .Action import Action
except:
    from Snapshot import Snapshot
    from .Action import Action
import shutil
from .SnapshotAgent import SnapshotAgent
from type.BaseData import TestConfig,ConnectionConfig
from .StandardTestObjectSetup import StandardTestObjectSetup

class SetupForTests(Action):    
    def explain(self,**kwargs):
        return """
        SetupForTests

        Set up a wallet, agent, and other important tools for testing
        """
    
    def prevalid(self,**kwargs):
        return True

    def run(self,**kwargs):
        assert kwargs['setup_type'] in ['ipfs','file','user']
        setup_type = kwargs['setup_type']
        return self.test_setup(setup_type=setup_type)
        return obj,new_cids 

    @staticmethod
    def test_setup(setup_type = 'ipfs') -> TestConfig:
        print("---- 1: Doing Setup")
        agent = SnapshotAgent()
        print("run_test.Calling 1 ")
        print(agent.create_wallet_action)
        decw, connected = agent.create_wallet_action(record={
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
        local_test_folder = './test/testdata/test_folder'
        backup_path='../devdecelium_backup/'    
        backup_path='../devdecelium_backup_test/'    
        # --- Remove old snapshot #
        try:
            shutil.rmtree(backup_path)
        except:
            pass
        conn_config = ConnectionConfig({
                    'local_test_folder':local_test_folder,
                    'decw':decw,
                    'connection_settings':connection_settings,
                    'backup_path':backup_path,
                    'user_context':user_context,
                    })
        print("setup_type",setup_type)
        object_setup:StandardTestObjectSetup = StandardTestObjectSetup()
        obj_id, decelium_path =  object_setup(
                    agent=agent,
                    conn_config=conn_config,
                    setup_type=setup_type)
        assert not 'error' in obj_id, "StandardTestObjectSetup Could not set up the object:" + str(obj_id)
        print("run_test",obj_id)
        assert decw.has_entity_prefix(obj_id)
        eval_context = {key: conn_config.get(key) for key in ['backup_path','self_id','connection_settings','decw']}
        eval_context['self_id'] = obj_id
        agent.evaluate_object_status(record={**eval_context,'target':'local','status':['object_missing','payload_missing']})
        agent.evaluate_object_status(record={**eval_context,'target':'remote','status':['complete']})
        agent.evaluate_object_status(record={**eval_context,'target':'remote_mirror','status':['complete']})

        test_config = TestConfig({**conn_config,
                'decelium_path':decelium_path,
                'obj_id':obj_id,
                'eval_context':eval_context
                })

        print("---- 2: Doing Small Pull")
        obj,new_cids = agent.append_object_from_remote(record=test_config)

        agent.evaluate_object_status(record={**eval_context,'target':'local','status':['complete']})
        agent.evaluate_object_status(record={**eval_context,'target':'remote','status':['complete']})
        agent.evaluate_object_status(record={**eval_context,'target':'remote_mirror','status':['complete']})
        return test_config
    
    def postvalid(self,**kwargs):
        return True