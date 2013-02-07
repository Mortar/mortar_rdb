# Copyright (c) 2011 Simplistix Ltd
# See license.txt for license details.
"""
Database independent provision of non-repeating, always-incrementing
sequences of integers.
"""
from mortar_rdb import get_session
from mortar_rdb.interfaces import ISequence
from zope.component import getSiteManager
from .generic import SequenceImplementation

def register_sequence(name, session, impl=SequenceImplementation):
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
       :class:`~mortar_rdb.interfaces.ISequence`.
       Defaults to
       :class:`mortar_rdb.sequence.generic.SequenceImplementation`.
       
    """
    getSiteManager().registerUtility(
        impl(name,session),
        provided=ISequence,
        name=name,
        ) 

def get_sequence(name):
    """
    Obtain a previously registered sequence.
    Once obtained, the :meth:`~mortar_rdb.interfaces.ISequence.next`
    method should be called as many times as necessary.

    Each call will return one system-wide unique integer that will be
    greater than any integers previously returned.
    """
    return getSiteManager().getUtility(ISequence,name)
