from sqlalchemy.orm.interfaces import SessionExtension

class VersionedOrTemporalExtension(SessionExtension):
    
    def before_flush(self, session, flush_context, instances):
        for instance in session.dirty:
            if not isinstance(instance, Versioned, Temporal):
                continue
            if not session.is_modified(instance, passive=True):
                continue

            if not attributes.instance_state(instance).has_identity:
                continue

            # make it transient
            instance.new_version(session)
            # re-add
            session.add(instance)
        for instance in session.deleted:
            if not isinstance(instance, Versioned, Temporal):
                continue
            # set valid_to
            pass

    def after_flush(self, session, flush_context):
        # if we don't have a ref, use the row id!
        
        mark_changed(session)
        
