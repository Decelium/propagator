try:
    from BaseData import BaseData
except:
    from .BaseData import BaseData

class TestCaseFunc(BaseData):
    def get_keys(self):
        required = {
                    'class':str,
                    'function':str,
                    'result':bool,
                    'output':str,
                    'error':str
                    }
        return required,{}

    def get_summary(self):
        return {k:self[k] for k in ['class','function','result'] }
