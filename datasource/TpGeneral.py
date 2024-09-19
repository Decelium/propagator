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
            # !!! Maps a datasource id with its corrisponding utility class !!!!
            #
            #
            local:TpGeneral = cls.Local         
            local_mirror:TpGeneral = cls.LocalMirror   # Note: LocalMirror seems unimplemented      
            remote:TpGeneral = cls.Decelium         
            remote_mirror:TpGeneral = cls.DeceliumMirror         

            cls.datasource_map = {
                            'local':local,
                            'local_attrib':local, 
                            'local_payload':local, 
                            'local_mirror':local_mirror,
                            'local_mirror_attrib':local_mirror, 
                            'local_mirror_payload':local_mirror, 
                            'remote':remote,
                            'remote_attrib':remote,
                            'remote_payload':remote, 
                            'remote_mirror':remote_mirror, 
                            'remote_mirror_attrib':remote_mirror,
                            'remote_mirror_payload':remote_mirror
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
