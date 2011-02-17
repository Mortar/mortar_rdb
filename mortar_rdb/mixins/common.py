import re
from sqlalchemy.ext.declarative import has_inherited_table
from sqlalchemy.util import classproperty

name_re = re.compile('([a-z]|^)([A-Z])')
def name_subber(match):
    if match.group(1):
        start = match.group(1)+'_'
    else:
        start = ''
    return start+match.group(2).lower()

class Common(object):

    @classproperty
    def __tablename__(cls):
        if has_inherited_table(cls):
            return None
        return name_re.sub(name_subber,cls.__name__)
    
    __table_args__ = {'mysql_engine':'InnoDB'}
    
