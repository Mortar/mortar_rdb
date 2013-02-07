# Copyright (c) 2011 Simplistix Ltd
# See license.txt for license details.
"""
Internal interface definitions.
Unless you're doing something pretty special, you don't need to know about these.
"""

from zope.interface import Interface

class ISession(Interface):
    """
    A marker interface for SQLAlchemy Sessions.
    This is so that we can register factories that return them.
    """

class ISequence(Interface):
    """
    An interface for sequence utility impementations.
    A sequence is a non-repeating, always-incrementing sequence of
    integers.

    Implementations of this interface will be instantiated
    once and then have their :meth:`next` method called often.
    """

    def __init__(name, session):
        """
        This function should do whatever needs to be done
        to make the sequence ready to use with the supplied
        name.

        It will be called by (ie: the class will be instantiated by)
        :func:`~mortar_rdb.sequence.register_sequence` and passed the name
        of the sequence and a :class:`~sqlalchemy.orm.session.Session`
        ready to use to check or create any data structures
        necessary.

        The session should not be retained, it should only be used
        within the body of the :meth:`__init__` method.
        """
        
    def next(session):
        """
        Return the next free integer in the sequence.
        It is the responsibility of the implementation of this
        interface to ensure that it can only ever return an
        integer once, no matter what happens.

        :param session:
          The :class:`~sqlalchemy.orm.session.Session` to use to
          obtain and store data needed to support the sequence
          implementation.
        """
        
