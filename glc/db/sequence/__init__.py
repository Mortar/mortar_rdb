"""
Database independent provision of non-repeating, always-incrementing
sequences of integers.
"""
from glc.db import getSession
from glc.db.interfaces import ISequence
from zope.component import getSiteManager
from .generic import SequenceImplementation

def registerSequence(name,session,impl=SequenceImplementation):
    """
    Register a sequence for later user.

    :param name:
       A string containing the name of the sequence.

    :param session:
       A :class:`~sqlalchemy.orm.session.Session` instance that
       will be used to set up anything needed in the database
       for the sequence to be functional. It will not be retained
       and may be closed and discarded once this function has
       returned.

    :param impl:
       A class whose instances implement
       :class:`~glc.db.interfaces.ISequence`.
       Defaults to
       :class:`glc.db.sequence.generic.SequenceImplementation`.
       
    """
    getSiteManager().registerUtility(
        impl(name,session),
        provided=ISequence,
        name=name,
        ) 

def getSequence(name):
    """
    Obtain a previously registered sequence.
    Once obtained, the :meth:`~glc.db.interfaces.ISequence.next`
    method should be called as many times as necessary.

    Each call will return one system-wide unique integer that will be
    greater than any integers previously returned.
    """
    return getSiteManager().getUtility(ISequence,name)
