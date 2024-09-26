
import traceback as tb
try:
    from ..Snapshot import Snapshot
    from .Action import Action
    from .SnapshotAgent import SnapshotAgent
    from ..type.BaseData import ConnectionConfig
except:
    from Snapshot import Snapshot
    from actions.Action import Action
    from actions.SnapshotAgent import SnapshotAgent
    from type.BaseData import ConnectionConfig


class StandardTestObjectSetup(Action):    
    def explain(self,**kwargs):
        return """
        StandardTestObjectSetup

        A quick action that uses Snapshot to append a specific object, from remote, to local. It verifies that the resulting directory 
        exists after downloading.
        """
    
    def prevalid(self,**kwargs):
        return True



    def run(self,**record):
        print("StandardTestObjectSetup.THE RECORD")
        print(record)
        return self.object_setup(agent=record['agent'],
            conn_config=record['conn_config'],
            setup_type=record['setup_type'])
    
    def postvalid(self,**kwargs):
        return True


    @staticmethod
    def object_setup(agent:SnapshotAgent,
                    conn_config:ConnectionConfig,
                    setup_type:str):
        
        
        print("object_setup.setup_type",setup_type)
        #assert setup_type in list(Snapshot.s_type_map.keys())
        user_context = conn_config.user_context()
        connection_settings = conn_config.connection_settings()
        local_test_folder = conn_config.local_test_folder()
        decw = conn_config.decw()
        result = decw.net.delete_entity(decw.dw.sr({'api_key':decw.dw.pubk("admin"),'path':'/corrupted_name'}))   
        result = decw.net.delete_entity(decw.dw.sr({'api_key':decw.dw.pubk("admin"),'path':'/temp/corrupted_name'}))   
        result = decw.net.delete_entity(decw.dw.sr({'api_key':decw.dw.pubk("admin"),'path':'/corrupted_name'}))   
        result = decw.net.delete_entity(decw.dw.sr({'api_key':decw.dw.pubk("admin"),'path':'/temp/corrupted_name'}))   

        if setup_type == 'ipfs':
            decelium_path = 'temp/test_folder.ipfs'
            ipfs_req_context = {**user_context, **{
                    'file_type':'ipfs', 
                    'connection_settings':connection_settings
            }}

            print("---- 2: Doing Small Upload")
            obj_id = agent.upload_directory_to_remote(record={
                'local_path': local_test_folder,
                'decelium_path': decelium_path,
                'decw': decw,
                'ipfs_req_context': ipfs_req_context,
                'user_context': user_context
            })
            return obj_id, decelium_path
        if setup_type == 'user':
            wallet_contents = decw.dw.get_raw()

            access_keys = wallet_contents["admin"]['user'].copy()
            access_keys['private_key'] = "destroy it"

            feature = {'username': "example_user",
                    'api_key': decw.dw.pubk("admin"),
                    'access_key':access_keys,
                    'password': "example_pass",
                    'password2': "example_pass",}
            result = decw.net.delete_entity(decw.dw.sr({'api_key':decw.dw.pubk("admin"),'path':'system_users','name':"example_user",}))   
            print("Delete result > "+str(result))
            decelium_path = 'system_users/example_user'
            obj_id = decw.net.user_register(feature)        
            
            assert decw.has_entity_prefix(obj_id), "Could not create the user "+str(obj_id)
            return obj_id, decelium_path
        
        assert setup_type in ['file','json','host','directory'], "Invalid setup_type detected "+ str(setup_type)
        if setup_type in ['file','json','host','directory']:
            if setup_type == 'directory':
                delete_requests = [{ 'path':'/example_dir'},{ 'path':'/corrupt_name'
                }]

                create_request = {
                    'path':'/',
                    'name':'example_dir',
                    'file_type':'directory',
                }
            
            elif setup_type == 'host':
                delete_requests = [{ 'path':'/example_domain.dns'},{ 'path':'/corrupt_name'
                }]

                create_request = {
                    'path':'/',
                    'name':'example_domain.dns',
                    'file_type':'host',
                    'attrib':{'host':'techoactivism.com',
                            'target_id':'xbj-INVALID_FOR_TESTING',
                                    'secret_password':"api_key"},
                }

            elif setup_type == 'file':
                delete_requests = [{ 'path':'/example_html_file_test.html'},{ 'path':'/corrupt_name'
                }]

                create_request = {
                    'path':'/',
                    'name':'example_html_file_test.html',
                    'file_type':'file',
                    'payload':'''<h1>This is a file</h1>''',
                }
            elif setup_type == 'json':
                delete_requests = [{ 'path':'/temp_dict.json'},{ 'path':'/corrupt_name'
                }]
                create_request = {
                    'path':'/',
                    'name':'temp_dict.json',
                    'file_type':'json',
                    'payload':{"example":"value"},
                }       
            else:
                raise Exception("Unsuported file type")
            #decelium_path = '/example_html_file_test.html'
            decelium_path = delete_request[0]['path']

            # download_try = decw.net.download_entity(decw.dw.sr({**user_context, **delete_request,'attrib':True}))
            # print("download_try\n",str(download_try))
            ##
            # Purge

            for delete_request in delete_requests:
                list_try = decw.net.list(decw.dw.sr({**user_context, **delete_request}))
                print("\n\n\nNew Test----------")
                print("\n\n\nNew Test----------")
                print("list_try\n",str(list_try))
                for sub_file in list_try:
                    del_inner = decw.net.delete_entity({**user_context, 'self_id':sub_file['self_id']})
                    assert del_inner == True , "Could not clean up an inner file "+ str(del_inner)
                singed_req = decw.dw.sr({**user_context, **delete_request})



                for i in range(1,3):
                    #if True == True:
                    del_try = decw.net.delete_entity(singed_req)
                    #print("TRIED DELETE in object_setup", del_try)
                    #obj = decw.net.download_entity({'path':create_request['path'],'name':create_request['name'],'attrib':True})
                    #print("Found Obj \n",obj)
                    try:
                        assert del_try == True  or ('error' in del_try and 'could not find' in del_try['error']), "Got an invalid response for del_try "+ str(del_try)
                    except Exception as e:
                        print("Failing Delete Object Id" + str(del_try))
                        raise e
            #raise Exception ("A GOOD RUN")
            # obj-1ecd6ba5-09e8-4f44-86f0-2c510349adc3
            ##
            # CREATE
            singed_req = decw.dw.sr({**user_context, **create_request})
            print(singed_req)
            obj = decw.net.download_entity({'path':singed_req['path'],'name':singed_req['name'],'attrib':True})
            print(obj)
            assert 'error' in obj, "Could not remove entity apparently. Entity: "+ str(obj)
            obj_id = decw.net.create_entity(singed_req)
            assert decw.has_entity_prefix(obj_id), "Could not create the object "+str(obj_id) + "delete res "+ str(del_try)
            return obj_id, decelium_path
        
        return {"error":"Did not register the existing type."}, None