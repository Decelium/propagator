

class ObjectMessages():
    allowed_tags = ["upload_fail"
               "download_fail"
               "incomplete_metadata"
               "incomplete_payload"
               "incomplete_payload"]
    def __init__(self,name,prior_errors= None):
        self.name = name
        self.tags = {}
        if  prior_errors != None:
            self.errors = prior_errors.copy()
        else:
            self.errors = []

    def append(self,messages):
        self.errors = self.errors + messages.errors()
        return True
    
    def add_assert(self,assert_condition,error_message,tags=None):
        if tags == None:
            tags = []
        for tag in tags:
            assert tag in ObjectMessages.allowed_tags
            self.tags[tag] = tag
        msg = {"result":assert_condition}
        if assert_condition != True:
            msg['name'] = self.name
            msg['error'] = error_message
            msg['tags'] = tags
            self.errors.append(msg)
            return False
        return True
    
    def get_error_messages(self):
        return self.errors
    
    def get_error_tags(self):
        return list(self.tags.keys())
    