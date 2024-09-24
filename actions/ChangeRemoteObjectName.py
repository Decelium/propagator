try:
    #from ..datasource.TpIPFSLocal import TpIPFSLocal
    #from ..Messages import ObjectMessages
    #from ..type.BaseData import BaseData,auto_c
    #from ..datasource.CorruptionData import CorruptionTestData
    from .Action import Action
except:
    #from Snapshot import Snapshot
    #from datasource.TpIPFSLocal import TpIPFSLocal
    #from Messages import ObjectMessages
    #from type.BaseData import BaseData,auto_c
    #from datasource.CorruptionData import CorruptionTestData
    from .Action import Action


class ChangeRemoteObjectName(Action):
    def explain(self,record,memory):
        pass
    def prevalid(self,record,memory):
        return True
    def postvalid(self,record,response,memory):
        decw = record['decw']
        user_context = record['user_context']
        self_id = record['self_id']
        dir_name = record['dir_name']
        assert response == True
        obj = TpIPFS.get_datasource("remote").load_entity({'api_key':'UNDEFINED',"self_id":self_id,'attrib':True},decw)

        assert obj['dir_name'] == dir_name   
        return True

    def run(self,record,memory):
        # TEST CASE: Corrupt the data ChangeRemoteObjectName
        decw = record['decw']
        user_context = record['user_context']
        decw = record['decw']
        dir_name = record['dir_name']
        self_id = record['self_id']

        singed_req = decw.dw.sr({**user_context, ## 
                'self_id':self_id,
                'attrib':{'dir_name':dir_name}
                })
        edit_try = decw.net.edit_entity(singed_req)
        return edit_try