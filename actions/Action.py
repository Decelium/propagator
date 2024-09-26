
import traceback as tb
'''
try:
    from ..datasource.TpIPFS import TpIPFS
    from ..Messages import ObjectMessages
    from ..type.BaseData import BaseData,auto_c
    #from ..type.CorruptionData import CorruptionTestData
except:
    from datasource.TpIPFS import TpIPFS
    from Messages import ObjectMessages
    from type.BaseData import BaseData,auto_c
    #from type.CorruptionData import CorruptionTestData
'''
def agent_action(**overrides):
    def decorator(run_func):
        class CustomAction(Action):
            def run(self, record, memory):
                return run_func(self, record, memory)

        for name, func in overrides.items():
            setattr(CustomAction, name, func)

        return CustomAction()
    return decorator

def memory_decorator(func):
    def wrapper(self, **kwargs):
        if self.__memory is None:
            self.__memory = {}
        kwargs['memory'] = self.__memory
        return func(self, **kwargs)
    return wrapper

class Action():
    
    def __init__(self,**kwargs):
        self.__memory = None
        if kwargs.get('memory', None) != None:
            self.__memory = kwargs['memory']

    def __call__(self, **kwargs):
        if self.__memory == None:
            self.__memory = {}
        kwargs['memory'] = self.__memory
        return self.crun(**kwargs)    
    
    #@memory_decorator
    def run(self,**kwargs):
        raise Exception("Unimplemented")
        return

    def prevalid(self,**kwargs):
        return True
    
    def postvalid(self,**kwargs):
        return True
    
    def crun(self,**kwargs):
        if self.__memory == None:
            self.__memory = {}
        if kwargs.get('memory', None)  == None:
            kwargs['memory'] = self.__memory

        err_str = "Unknown Error"
        try:
            assert self.prevalid(**kwargs) #record, memory
            kwargs['response'] = self.run(**kwargs)  #record, memory
            print("Action.crun.self:"+str(self))
            assert self.postvalid(**kwargs)  #record, memory, response
            return kwargs['response']
        except Exception as e :
            # Package up a highly detailed exception log for record keeping
            exc = tb.format_exc()
            err_args = kwargs.copy()
            if 'response' in kwargs:
                del(kwargs['response'])
            goal_text = self.explain(**kwargs)  #record, memory
            err_str = "Encountered an exception when seeking action:\n\n "
            err_str += goal_text
            #err_str += "\n\nException:\n\n"
            #err_str += exc
            print(err_str)
            raise e

    def test(self):
        return True

    def explain(self,**kwargs):
        return ""
    
    def generate(self,**kwargs):
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

