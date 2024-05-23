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
    def get_datasource_refac(cls,type_id:str) -> TpGeneral:
        if (cls.datasource_map == None):
            # TODO -- come up with a sensible and consistent mapping strategy
            cls.datasource_map = {
                            'local':cls.Local,
                            'local_attrib':cls.Local, #??? -- Seems innaccurate
                            'local_payload':cls.Local, #??? -- Seems innaccurate
                            'local_mirror':cls.LocalMirror,
                            'local_mirror_attrib':cls.LocalMirror, #??? -- Seems innaccurate
                            'local_mirror_payload':cls.LocalMirror, #??? -- Seems innaccurate
                            'remote':cls.Decelium,
                            'remote_attrib':cls.Decelium, #??? -- Seems innaccurate
                            'remote_payload':cls.Decelium, #??? -- Seems innaccurate
                            'remote_mirror':cls.DeceliumMirror, 
                            'remote_mirror_attrib':cls.DeceliumMirror, #??? -- Seems innaccurate
                            'remote_mirror_payload':cls.DeceliumMirror #??? -- Seems innaccurate
                            }    
        assert type_id in list(cls.datasource_map.keys()), "could not find "+ type_id + " in datasource_map"
        return cls.datasource_map[type_id]
    class Local(TpGeneral):
        pass
    class LocalMirror(TpGeneral):
        pass
    class Decelium(TpGeneral):
        pass
    class DeceliumMirror(TpGeneral):
        pass
