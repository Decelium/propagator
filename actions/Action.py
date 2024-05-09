
import traceback as tb
try:
    from ..datasource.TpIPFSDecelium import TpIPFSDecelium
    from ..datasource.TpIPFSLocal import TpIPFSLocal
    from ..Messages import ObjectMessages
    from ..type.BaseData import BaseData,auto_c
    #from ..type.CorruptionData import CorruptionTestData
except:
    from datasource.TpIPFSDecelium import TpIPFSDecelium
    from datasource.TpIPFSLocal import TpIPFSLocal
    from Messages import ObjectMessages
    from type.BaseData import BaseData,auto_c
    #from type.CorruptionData import CorruptionTestData

def agent_action(**overrides):
    def decorator(run_func):
        class CustomAction(Action):
            def run(self, record, memory):
                return run_func(self, record, memory)

        for name, func in overrides.items():
            setattr(CustomAction, name, func)

        return CustomAction()
    return decorator

class Action():
    def __init__(self,memory = None):
        if memory == None:
            self.__memory = {}
        else:
            self.__memory = memory.copy()

    def __call__(self, record, memory=None):
        if memory == None:
            memory = {}
        return self.crun(record, memory)    
      
    def run(self,record,memory):
        raise Exception("Unimplemented")
        return

    def prevalid(self,record,memory):
        return True
    
    def postvalid(self,record,response,memory):
        return True
    
    def crun(self,record,memory=None):
        if memory == None:
            memory = {}
        err_str = "Unknown Error"
        try:
            assert self.prevalid(record,memory)
            response = self.run(record,memory)
            assert self.postvalid(record,response,memory)
            return response
        except Exception as e :
            # Package up a highly detailed exception log for record keeping
            exc = tb.format_exc()
            goal_text = self.explain(record,memory)
            err_str = "Encountered an exception when seeking action:\n\n "
            err_str += goal_text
            #err_str += "\n\nException:\n\n"
            #err_str += exc
            print(err_str)
            raise e

    def test(self):
        return True

    def explain(self,record,memory):
        return ""
    
    def generate(self,lang,record,memory):
        return ""
    
class ExampleAction(Action):
    def run(self,record,memory):
        raise Exception("Unimplemented")
        return
    def prevalid(self,record,memory):
        return True
    def postvalid(self,record,response,memory):
        return True
    def explain(self,record,memory):
        return ""
    def test(self):
        return True
    def generate(self,lang,record,memory):
        return ""    

