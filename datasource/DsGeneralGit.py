import decelium_wallet.core as core
import pandas
try:
    from Messages import ObjectMessages
except:
    from ..Messages import ObjectMessages
import traceback as tb
import ipfshttpclient
from .DsGeneral import DsGeneral
from .DsGeneralLocal import DsGeneralLocal
import os
from dulwich import client, repo 
from dulwich.objects import Blob
from dulwich.walk import Walker

import datetime

class jsondateencode_local:
    def loads(dic):
        return json.loads(dic,object_hook=jsondateencode_local.datetime_parser)
    def dumps(dic):
        return json.dumps(dic,default=jsondateencode_local.datedefault)

    def datedefault(o):
        if isinstance(o, tuple):
            l = ['__ref']
            l = l + o
            return l
        if isinstance(o, (datetime.date, datetime.datetime,)):
            return o.isoformat()

    def datetime_parser(dct):
        DATE_FORMAT = '%Y-%m-%dT%H:%M:%S'
        for k, v in dct.items():
            if isinstance(v, str) and "T" in v:
                try:
                    dct[k] = datetime.datetime.strptime(v, DATE_FORMAT)
                except:
                    pass
        return dct

class DsGeneralGit(DsGeneral):
    @classmethod
    def download_ipfs_data(cls,TpDestination,decw,cids, download_path, connection_settings,overwrite=False,failure_limit=5):
        raise Exception("Unsupported")
                

    @classmethod
    def download_git_data(cls, TpDestination, decw, download_path, connection_settings, overwrite=False, failure_limit=5):
        c = connection_settings
        repo_url = c['repo_url']
        repo_branch = c.get('repo_branch', 'master')  # Default to 'master' if not specified
        repo_hash = c.get('repo_hash')  # Specific commit hash, if provided

        failures = 0
        count = 0

        # Create an in-memory repository
        mem_repo = repo.MemoryRepo()

        # Set up the client to fetch from the remote repository
        remote_client, path = client.get_transport_and_path(mem_repo, repo_url)

        # Fetch the refs from the remote repository
        try:
            remote_refs = remote_client.fetch(path, mem_repo)
        except Exception as e:
            print(f"Failed to fetch refs from {repo_url}: {e}")
            return {'error': f"Failed to fetch refs from {repo_url}"}

        # Determine which ref to use
        if repo_hash:
            commit_sha = repo_hash.encode('utf-8')
        else:
            # Convert branch name to ref name
            ref_name = f'refs/heads/{repo_branch}'
            commit_sha = remote_refs.get(ref_name.encode('utf-8'))
            if not commit_sha:
                # Try fetching from remote heads
                ref_name = f'refs/remotes/origin/{repo_branch}'
                commit_sha = remote_refs.get(ref_name.encode('utf-8'))
            if not commit_sha:
                print(f"Failed to find branch {repo_branch} in {repo_url}")
                return {'error': f"Failed to find branch {repo_branch}"}

        # Walk the repository tree to collect all files
        walker = Walker(mem_repo.object_store, [commit_sha], include_trees=True, prune=False)
        file_entries = []

        for entry in walker:
            tree = entry.tree
            if not tree:
                continue
            for path, blob_sha in tree.items():
                obj = mem_repo[blob_sha]
                if isinstance(obj, Blob):
                    file_path = path.decode('utf-8')
                    file_entries.append({
                        'path': file_path,
                        'sha': blob_sha.hexdigest(),
                        'mode': oct(obj.mode),
                        'size': obj.raw_length(),
                        'blob': obj
                    })

        total_files = len(file_entries)
        print(f"Found {total_files} files to backup.")

        # Process each file
        for file_info in file_entries:
            count += 1
            file_path = file_info['path']
            file_sha = file_info['sha']
            file_mode = file_info['mode']
            file_size = file_info['size']
            blob = file_info['blob']

            # Construct metadata dictionary
            dic = {
                'path': file_path,
                'sha': file_sha,
                'mode': file_mode,
                'size': file_size,
                'commit_sha': commit_sha.decode('utf-8')
            }

            # Get the file content
            file_content = blob.data

            print(f"{count}/{total_files} - Backing up {file_path}")

            # Since backup_git_entity expects a URL or file content,
            # we can pass the file content directly
            TpDestination: DsGeneralLocal = TpDestination
            new_local_files = TpDestination.backup_git_entity(DsGeneralGit, dic, file_content, download_path, overwrite)

            # Handle failures
            if not new_local_files:
                failures += 1
                if failures > failure_limit > 0:
                    print("Hit Failure Limit")
                    return {'error': "Too many failures"}

        return True


    @classmethod
    def download_payload_data(cls,decw,obj):
        '''
        obj = {
            'self_id':'obj-378543820a0f',
            'url':'https://github.com/Decelium/decelium_wallet',
        }
        '''
        raise Exception("Unimplemented")        

        
    @classmethod
    def get_cid_read_stream(cls,client,root_cid):
        raise Exception("unimplemented")
    
    @classmethod
    def load_entity(cls,query,decw):
        assert 'api_key' in query
        assert 'self_id' in query or 'path' in query 
        assert 'attrib' in query
        return decw.net.download_entity(query)

    @classmethod
    def download_directory_dag(cls,client, cid, path=""):
        item_details_response = client.object.get(cid)
        item_details = {
            'Links': [{
                'Name': link['Name'],
                'Hash': link['Hash'],
                'Size': link['Size']
            } for link in item_details_response['Links']]
        }
        return item_details

    @classmethod
    def validate_object(cls,decw,object_id,download_path,connection_settings,obj_remote = None):
        entity_success,entity_messages = cls.validate_object_attrib(decw,object_id,download_path,connection_settings)
        payload_success,payload_messages = cls.validate_object_payload(decw,object_id,download_path,connection_settings)
        entity_messages:ObjectMessages = entity_messages
        all_messages:ObjectMessages = payload_messages
        all_messages.append(entity_messages)
        return entity_success and payload_success,all_messages
    
    
    @classmethod
    def validate_object_attrib(cls,decw,object_id,download_path,connection_settings,obj_remote = None):
        messages = ObjectMessages("DsGeneralDecelium.validate_object_entity(for {"+object_id+"})")
        obj_valid = decw.net.validate_entity_hash( {'api_key':'UNDEFINED', 'self_id':object_id})
        if messages.add_assert(obj_valid == True, f"validate_entity_hash({object_id}) seems to have an invalid hash, as reported by DB validate_object_entity:"+str(obj_valid)) == False:
            return False, messages
        return len(messages.get_error_messages()) == 0, messages      

   
    @classmethod
    def validate_object_payload(cls,decw,object_id,download_path,connection_settings,obj_remote = None):
        messages = ObjectMessages("DsGeneralDecelium.validate_object_payload(for {"+object_id+"})")
        if obj_remote == None:
            obj_remote = decw.net.download_entity( {'api_key':'UNDEFINED', 'self_id':object_id,'attrib':True})

        obj_valid = decw.net.validate_entity_hash( {'api_key':'UNDEFINED', 'self_id':object_id})
        if messages.add_assert(obj_valid == True, f"B. validate_entity_hash({object_id}) seems to have an invalid hash, as reported by DB validate_object_entity:"+str(obj_valid)) == False:
            return False, messages

        
        cids_pinned = []
        for k in ['self_id','parent_id','dir_name','settings']:
            if messages.add_assert(k in obj_remote and obj_remote[k] != None, "missing {k} for {object_id}") == False:
                return False, messages
        
        if messages.add_assert('ipfs_cid' in obj_remote['settings'], "missing settings.ipfs_cid for {object_id}"):
            cids_pinned.append (obj_remote['settings']['ipfs_cid'] )

        messages.add_assert('ipfs_name' in obj_remote['settings'], "missing settings.ipfs_name for {object_id}")
        if 'ipfs_cids' in obj_remote['settings']:
            for key in obj_remote['settings']['ipfs_cids'].keys():
                if messages.add_assert(key in obj_remote['settings']['ipfs_cids'], "missing {key} from settings.ipfs_cids for {object_id}"):
                    cids_pinned.append (obj_remote['settings']['ipfs_cids'][key] )
        first = True
        for cid in cids_pinned:
            result = decw.net.check_pin_status({
                    'api_key':"UNDEFINED",
                    'connection_settings':connection_settings,
                    'cid': cid,
                    'do_refresh':first})
            first = False
            messages.add_assert(result == True, "cid is missing from remote "+cid)

        return len(messages.get_error_messages()) == 0, messages  

    @classmethod
    def ipfs_pin_list(cls,decw, connection_settings,refresh=False):
        pins = decw.net.download_pin_status({
                'api_key':"UNDEFINED",
                'do_refresh':refresh,
                'connection_settings':connection_settings})       
        assert not 'error' in pins
        return pins
    
    @classmethod
    def upload_path_to_ipfs(cls,decw,connection_settings,payload_type,file_path):
        result = decw.net.create_ipfs({
                'api_key':"UNDEFINED",
                'file_type':'ipfs', 
                'connection_settings':connection_settings,
                'payload_type':payload_type,
                'payload':file_path})
        return result
    
    @classmethod
    def find_download_entity(cls,decw,offset=0,limit=20):
        found = []
        returned = limit
        while returned >= limit:
            print('.',end="")
            docs,returned = cls.find_batch_cids(decw,offset,limit)
            offset = offset + limit
            found = found + docs
        return found
        
    @classmethod
    def is_directory(cls,decw,connection_settings,hash):
        # Example pseudocode for IPFS. You'll need to adapt this based on your IPFS client library or command line usage
        _,ipfs_api = decw.net.create_ipfs_connection(connection_settings)
        object_info = ipfs_api.ls(hash)
        object_info = object_info['Objects'][0]
        
        if object_info and 'Links' in object_info:
            for link in object_info['Links']:
                if len(link['Name']) > 0:
                    return True
            # return len(object_info['Links']) > 0  # Has links, likely a directory
        return False  # No links, likely a file    pass
    
    @classmethod
    def find_batch_object_ids(cls,decw,offset,limit,filter=None):
        if filter ==None:
            filter ={'attrib':{'file_type':'ipfs'}}
        filter['limit'] = limit
        filter['offset'] = offset
        docs = decw.net.list(filter)
        obj_ids = []
        for doc in docs:
            obj_ids.append(doc['self_id'])
        return obj_ids

    @classmethod
    def find_batch_objects(cls,decw,offset,limit,filter=None):
        if filter ==None:
            filter ={'attrib':{'file_type':'ipfs'}}
        #print("Searching Filter = "+str(filter))
        filter['limit'] = limit
        filter['offset'] = offset
        docs = decw.net.list(filter)
        if type(docs) == dict and 'error' in docs:
            return filter
        #print(docs)
        obj_ids = []
        for doc in docs:
            obj_ids.append(doc)
        return obj_ids

    
    @classmethod
    def find_batch_cids(cls,decw,offset,limit,filter=None):
        found = []
        if filter ==None:
            filter ={'attrib':{'file_type':'ipfs'}}
        filter['limit'] = limit
        filter['offset'] = offset
        docs = decw.net.list(filter)
        returned = len(docs)
        for doc in docs:
            if 'settings' in doc.keys():
                if 'ipfs_cid' in doc['settings'].keys():
                    rec = {"self_id":doc['self_id']}
                    rec['cid'] = doc['settings']['ipfs_cid']
                    found.append(rec)
                if 'ipfs_cids' in doc['settings'].keys():
                    for pin in doc['settings']['ipfs_cids'].values():
                        rec = {"self_id":doc['self_id'],"cid":pin}
                        found.append(rec)                
        return found,returned
    
    @classmethod
    def decelium_has_cids(cls,decw,new_cids):
        all_cids = cls.find_all_cids(decw)
        df = pandas.DataFrame(all_cids)
        all_cids = list(df['cid'])
        is_subset = set(new_cids) <= set(all_cids)
        return is_subset
    
    @classmethod
    def ipfs_has_cids(cls,decw,new_cids, connection_settings,refresh=False):
        all_cids = cls.ipfs_pin_list(decw, connection_settings,refresh)
        is_subset = set(new_cids) <= set(all_cids)
        #if not is_subset:
        #    print("Missing some CIDS from subset")
        #    print(set(new_cids) - set(all_cids))
        return is_subset
    
    @classmethod
    def find_all_cids(cls,decw,offset=0,limit=20): #find all cids on decelium
        found = []
        returned = limit
        while returned >= limit:
            print('.',end="")
            docs,returned = cls.find_batch_cids(decw,offset,limit)
            offset = offset + limit
            found = found + docs
        return found
    
class DsAttribDecelium(DsGeneralDecelium):
    @classmethod 
    def reupload_payload(cls,decw,obj):
        messages = ObjectMessages("TpAttrib.Decelium.reupload_payload_stub")
        # I think we have nothing to do, as no payload really exists
        return None, messages
    
    @classmethod
    def validate_object(cls,decw,object_id,download_path,connection_settings,obj_remote = None):
        entity_success,entity_messages = cls.validate_object_attrib(decw,object_id,download_path,connection_settings)
        return entity_success,entity_messages

    @classmethod
    def validate_object_attrib(cls,decw,object_id,download_path,connection_settings,obj_remote = None):
        messages = ObjectMessages("DsFileDecelium.validate_object_entity_mirror(for {"+object_id+"})")
        obj_valid = decw.net.validate_entity_hash( {'api_key':'UNDEFINED', 'self_id':object_id})
        if messages.add_assert(obj_valid == True, f"validate_entity_hash({object_id}) seems to be invalid, as reported by DB validate_object_entity_mirror:"+str(obj_valid)) == False:
            return False, messages
        return len(messages.get_error_messages()) == 0, messages

    @classmethod
    def validate_object_payload(cls,decw,object_id,download_path,connection_settings,obj_remote = None):
        messages = ObjectMessages("TpAttrib.Decelium.validate_object_payload(for {"+object_id+"})")
        return None, messages 
    

class DsFileDecelium(DsGeneralDecelium):
    @classmethod 
    def reupload_payload(cls,decw,obj):
        assert 'self_id' in obj
        assert 'payload' in obj
        
        messages = ObjectMessages("DsFileDecelium.reupload_payload(for {"+obj['self_id']+"})")
        print("TpFile.reupload_payload.reupload_entity_payload CALL"+str(obj))
        res = decw.net.reupload_entity_payload({'self_id':obj['self_id'],'payload':obj['payload']})
        print("TpFile.reupload_payload.reupload_entity_payload result")
        print(res)
        if res == True:
            return True, messages
            
        messages.add_assert(res not in [False,None,0], f"Null reupload error occoured uploading "+str(obj['self_id']))
        if type(res) == dict:
            messages.add_assert('error' not in res, f"Error occoured reuploading "+str(res))
        
        return False, messages
    
    @classmethod
    def validate_object(cls,decw,object_id,download_path,connection_settings,obj_remote = None):
        entity_success,entity_messages = cls.validate_object_attrib(decw,object_id,download_path,connection_settings)
        payload_success,payload_messages = cls.validate_object_payload(decw,object_id,download_path,connection_settings)
        entity_messages:ObjectMessages = entity_messages
        all_messages:ObjectMessages = payload_messages
        all_messages.append(entity_messages)
        return entity_success and payload_success,all_messages

    @classmethod
    def validate_object_attrib(cls,decw,object_id,download_path,connection_settings,obj_remote = None):
        messages = ObjectMessages("TpFileDecelium.validate_object_entity_mirror(for {"+object_id+"})")
        obj_valid = decw.net.validate_entity_hash( {'api_key':'UNDEFINED', 'self_id':object_id})
        if messages.add_assert(obj_valid == True, f"validate_entity_hash({object_id}) seems to be invalid, as reported by DB validate_object_entity_mirror:"+str(obj_valid)) == False:
            return False, messages
        return len(messages.get_error_messages()) == 0, messages      

    @classmethod
    def validate_object_payload(cls,decw,object_id,download_path,connection_settings,obj_remote = None):
        messages = ObjectMessages("TpFileDecelium.validate_object_payload(for {"+object_id+"})")
        if obj_remote == None:
            obj_remote = decw.net.download_entity( {'api_key':'UNDEFINED', 'self_id':object_id,'attrib':True})

        obj_valid = decw.net.validate_entity_hash( {'api_key':'UNDEFINED', 'self_id':object_id})
        if messages.add_assert(obj_valid == True, f"B. validate_entity_hash({object_id}) seems to have an invalid hash, as reported by DB validate_object_entity:"+str(obj_valid)) == False:
            return False, messages

        for k in ['self_id','parent_id','dir_name','settings']:
            if messages.add_assert(k in obj_remote and obj_remote[k] != None, "missing {k} for {object_id}") == False:
                return False, messages
        
        messages.add_assert('region' in obj_remote['settings'], "missing settings.region for {object_id}")
        messages.add_assert('bucket' in obj_remote['settings'], "missing settings.region for {object_id}")
        payload_data = decw.net.download_entity( {'api_key':'UNDEFINED', 'self_id':object_id})
        if type(payload_data) is dict and 'error' in payload_data and payload_data['error'] == 'This file is empty':
            messages.add_assert(False, "Payload is missing.")

        return len(messages.get_error_messages()) == 0, messages  

