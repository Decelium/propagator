class TpGeneral():
    # TODO - add types in return values func()->
    @classmethod
    def validate_object(cls,decw,object_id,download_path,connection_settings,obj_remote = None):
        raise Exception("Unimplemented")

    @classmethod
    def validate_object_attrib(cls,decw,object_id,download_path,connection_settings,obj_remote = None):
        raise Exception("Unimplemented")

    @classmethod
    def validate_object_payload(cls,decw,object_id,download_path,connection_settings,obj_remote = None):
        raise Exception("Unimplemented")


class TpFacade:
    datasource_map = None
    @classmethod
    def get_datasource(cls,type_id:str):
        if (cls.datasource_map == None):
            cls.datasource_map = {
                            'local':cls.Local,
                            'local_attrib':cls.Local,
                            'local_payload':cls.Local,
                            'local_mirror':cls.LocalMirror,
                            'local_mirror_attrib':cls.LocalMirror,
                            'local_mirror_payload':cls.LocalMirror,
                            'remote':cls.Decelium,
                            'remote_attrib':cls.Decelium,
                            'remote_payload':cls.Decelium,
                            'remote_mirror':cls.DeceliumMirror,
                            'remote_mirror_attrib':cls.DeceliumMirror,
                            'remote_mirror_payload':cls.DeceliumMirror
                            }    
        assert type_id in list(cls.datasource_map.keys())
        return cls.datasource_map[type_id]
    class Local(TpGeneral):
        pass
    class LocalMirror(TpGeneral):
        pass
    class Decelium(TpGeneral):
        pass
    class DeceliumMirror(TpGeneral):
        pass
