import decelium_wallet.core as core
import ipfshttpclient
import os
import json
import pprint

class Migrator():

    @staticmethod
    def is_directory(decw,connection_settings,hash):
        # Example pseudocode for IPFS. You'll need to adapt this based on your IPFS client library or command line usage
        _,ipfs_api = decw.net.create_ipfs_connection(connection_settings)
        object_info = ipfs_api.ls(hash)
        object_info = object_info['Objects'][0]
        # print(object_info)
        if object_info and 'Links' in object_info:
            print("Inspecting links")
            for link in object_info['Links']:
                if len(link['Name']) > 0:
                    return True
            # return len(object_info['Links']) > 0  # Has links, likely a directory
        return False  # No links, likely a file

    @staticmethod
    def find_batch_cids(decw,offset,limit):
        found = []
        docs = decw.net.list({'limit':limit,'offset':offset,'attrib':{'file_type':'ipfs'}})
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

    @staticmethod
    def find_all_cids(decw,offset=0,limit=20):
        found = []
        returned = limit
        while returned >= limit:
            print('.',end="")
            docs,returned = Migrator.find_batch_cids(decw,offset,limit)
            offset = offset + limit
            found = found + docs
        print('')
        return found

    @staticmethod
    def find_download_entity(decw,offset=0,limit=20):
        found = []
        returned = limit
        while returned >= limit:
            print('.',end="")
            docs,returned = Migrator.find_batch_cids(decw,offset,limit)
            offset = offset + limit
            found = found + docs
        return found

    def backup_ipfs_entity(item,current_pins,download_path,client):
        new_cids = []
        cid = item['cid']
        file_path = os.path.join(download_path, cid)

        # Check if the file already exists to avoid double writing
        if os.path.exists(file_path+".file") or os.path.exists(file_path+".dag"):
            print(f"CID {cid} already exists in {file_path}")
            return new_cids

        try:
            # Check if the item is pinned on this node
            pinned = False
            if cid in current_pins['Keys']:
                pinned = True
            if not pinned:
                print(f"CID {cid} IS NOT PINNED {file_path}")
                return new_cids

            # If pinned, proceed to download
            try:
                res = client.cat(cid)
                with open(file_path+".file", 'wb') as f:
                    f.write(res)
                print(f"Downloaded {cid} to file {file_path}")
            except Exception as e:
                if "is a directory" in str(e):
                    print(f"Downloaded {cid} to dir {file_path}")
                    dir_json = Migrator.backup_directory_dag(client,cid)
                    for new_item in dir_json['Links']:
                        #print(item)
                        #print(dir_json)
                        new_cids.append({'self_id':item['self_id'],'cid':new_item['Hash']})
                    # dir_json = client.object.get(cid)
                    print(json.dumps(dict(dir_json)))
                    with open(file_path+".dag", 'w') as f:
                        f.write(json.dumps(dir_json))
                    print("Finished Directory")
                else:
                    raise e
            return new_cids
        except Exception as e:
            import traceback as tb
            print(f"Error downloading {cid}: {e}")
            print(tb.format_exc())
            return new_cids

    @staticmethod
    def download_ipfs_data(docs, download_path, connection_settings):
        c = connection_settings
        # Ensure the download directory exists
        if not os.path.exists(download_path):
            os.makedirs(download_path)
        ipfs_string = f"/dns/{c['host']}/tcp/{c['port']}/{c['protocol']}"

        current_docs = docs
        next_batch = []
        with ipfshttpclient.connect(ipfs_string) as client:
            try:
                pins = client.pin.ls(type='recursive')
            except Exception as pin_check_error:
                print(f"Error checking pin status for {cid}: {pin_check_error}")
            while len(current_docs) > 0:
                for item in current_docs:
                    dic = item
                    if type(item) == str:
                        dic = {'cid':item,'self_id':None}
                    new_pins = Migrator.backup_ipfs_entity(dic,pins,download_path,client)
                    if len(new_pins) > 0:
                        next_batch = next_batch + new_pins
                print("Moving to new batch-------------")
                current_docs = next_batch
                next_batch = []

    @staticmethod
    def backup_directory_dag(client, cid, path=""):
        item_details_response = client.object.get(cid)
        item_details = {
            'Links': [{
                'Name': link['Name'],
                'Hash': link['Hash'],
                'Size': link['Size']
            } for link in item_details_response['Links']]
        }

        return item_details