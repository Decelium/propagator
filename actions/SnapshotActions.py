'''
An Agent action is a generic system/bot level activity with an expected outcome. What makes 
an action of this type special, is that it couples semantics, implementation, validation, and stubbed testing, all together in one structure.

As a developer, bundling actions in this way *If I am right* allows for a developer to tackle many tasks at once:
- User Documentation:
        - Docs can be generated by using explain() and generate(), to explain an action and generate example code
- Unit tests:
    - Unit tests can be easily crafted by run()ing the code, and then using postvalid() which should assert correctness
    - when things fail, it should also be clearer in logs, what SEMANTIC action broke, 
- AI delegation: 
    - It should be possible to show an LLM many actions in a library so it can
        - Create more actions
        - String actions together into multi-step processes
        - assist more fully in high level planning, as assembling "actions" is easier than assembling code
- Junior delegation. 
    - Same as AI, I'm afraid.
- Tutorial generation: One can generate tutorials, by wriring an intro, then explaining() and generating() several actions
- Optimal solutions: 
    - One can generate() whole action graphs into transpiled applications
- Example Libraries:
    - It should be possible to create Example libraries (like github dists) simply by explain()ing many examples
- Bots, and bot monitoring:
    - When running a bot, it should be easier to express the semantics of a bot
    - It will be easier to run a bot in a fully "valid" mode, and an lean run() only mode
    - If a bot is failing in code, it will be easy to contextualize its failues as the intended action can be extracted

Overall, by packing semantics with code, it may be possible to kill 7 birds with one stone. As is the case with all 
amazing great ideas, I look forward to seeing what I am wrong about.

----
When to use this:

This framework IS NOT for building apps in. Truely this is a bloated and detail oriented system. This system should be used 
if you are shipping a software tool / library, and are looking to maintain perfect tracibility between:
- Unit Tests
- End-to-end Tests
- Bots / Auotmations / Backup scripts
- Documentation

If you have a just one or two of the above use cases CriticalAgent is likely the wrong tool. It will be bloated, and frankly, wasteful. 
However, if you are a solo developer, and you have written a library, and you need to churn out unit tests, onboarding guides, examples, and need to 
run backup cron jobs, then CriticalAgent might save you. You can write one ActionTree, and generate all the docs and system agents you need.

Once your agent graphs exist you can:
1) Mix and match actions over action trees
2) Run bots, and get clear reports from bots (vs deep diving in log hell)
3) As needed, generate docs and guides,
4) As you update your code base, your docs / agents will always be in sync

'''
import traceback as tb

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
    def __init__(self):
        self.__memory = {}
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
        except:
            # Package up a highly detailed exception log for record keeping
            exc = tb.format_exc()
            goal_text = self.explain(record,memory)
            err_str = "Encountered an exception when seeking action:\n\n "
            err_str += goal_text
            err_str += "Exception:\n\n"
            err_str += exc
            raise Exception(err_str)

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

import decelium_wallet.core as core

class CreateDecw(Action):
    def run(self,record,memory):
        decw = core()
        with open(record['wallet_path'],'r') as f:
            data = f.read()
        with open(record['wallet_password_path'],'r') as f:
            password = f.read()
        loaded = decw.load_wallet(data,password)
        assert loaded == True
        connected = decw.initial_connect(target_url=record['fabric_url'],
                                        api_key=decw.dw.pubk())
        return decw,connected

    def prevalid(self,record,memory):
        assert 'wallet_path' in record
        assert 'wallet_password_path' in record
        assert 'fabric_url' in record        

        return True
    
    def postvalid(self,record,response,memory=None):
        assert type(response[0]) == core
        assert response[1] == True
        return True
   
    def test(self,record):
        return True

    def explain(self,record,memory=None):
        result = '''
        CreateDecw

        Standard initialization work for decelium. This code loads a wallet from a path, 
        and establishes a connection with a miner. If it succeeds, it means a wallet was indeed
        loaded, and that a connection to a local or remote miner has been established.

        '''
        return result
    
    def generate(self,lang,record,memory=None):
        return ""
    
