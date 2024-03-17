class Messages():
    def __init__(self,name,prior_errors= None):
        self.name = name
        if  prior_errors != None:
            self.errors = prior_errors.copy()
        else:
            self.errors = []

    def append(self,messages):
        self.errors = self.errors + messages.errors()
        return True
    
    def add_assert(self,assert_condition,error_message):
        msg = {"result":assert_condition}
        if assert_condition != True:
            msg['name'] = self.name
            msg['error'] = error_message
            self.errors.append(msg)
            return False
        return True
    
    def get_error_messages(self):
        return self.errors
    