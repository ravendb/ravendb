#-----------------------------------------------------------------------
# <copyright file="esedbshelf.py" company="Microsoft Corporation">
# Copyright (c) Microsoft Corporation.
# </copyright>
#-----------------------------------------------------------------------

"""
Provides shelve functionality backed by an ESENT database. See the
documentation in the shelve module for full details.
"""

import shelve
import StringIO
import pickle
import esedb

#-----------------------------------------------------------------------
class EseDbShelf(shelve.Shelf):
#-----------------------------------------------------------------------
    """
    A persistent, dictionary-like object backed by an ESENT database.
    The values (not the keys!) of a shelf can be essentially arbitrary
    Python objects - anything that the pickle module can handle. This
    includes most class instances, recursive data types, and objects
    containing lots of shared sub-objects. The keys are ordinary strings.
    
    >>> s = EseDbShelf('wdbtest.db', 'n', None, True)
    >>> for i in range(10): s['%d'%i] = (i, i*i, '#%d#'%i)
    ...
    >>> s['3']
    (3, 9, '#3#')
    >>> s.keys()
    ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']
    >>> s.first()
    ('0', (0, 0, '#0#'))
    >>> s.next()
    ('1', (1, 1, '#1#'))
    >>> s.last()
    ('9', (9, 81, '#9#'))
    >>> s.set_location('3')
    ('3', (3, 9, '#3#'))
    >>> s.previous()
    ('2', (2, 4, '#2#'))
    >>> for k, v in s.iteritems():
    ...     print k, v
    0 (0, 0, '#0#')
    1 (1, 1, '#1#')
    2 (2, 4, '#2#')
    3 (3, 9, '#3#')
    4 (4, 16, '#4#')
    5 (5, 25, '#5#')
    6 (6, 36, '#6#')
    7 (7, 49, '#7#')
    8 (8, 64, '#8#')
    9 (9, 81, '#9#')
    >>> '8' in s
    True
    >>> s.sync()
    >>> s.close()
    
    """    

    def __init__(self, filename, flag='cf', protocol=None, writeback=False):
        shelve.Shelf.__init__(self, esedb.open(filename, flag), protocol, writeback)

    def set_location(self, key):
        """Sets the cursor to the entry specified by the key and returns
        a pair (key, value) for the entry.
        
        >>> x = open('shelve.db', flag='nf')
        >>> x['key'] = (1, 'bar')
        >>> x.set_location('key')
        ('key', (1, 'bar'))
        >>> x.close()        
        
        If the key doesn't exist in the database then the location is set
        to the next highest key and that record is returned.
        
        >>> x = open('shelve.db', flag='nf')
        >>> x['b'] = (2, 'baz')
        >>> x.set_location('a')
        ('b', (2, 'baz'))
        >>> x.close()                
        
        If no matching key is found then KeyError is raised.

        >>> x = open('shelve.db', flag='nf')
        >>> x['a'] = 'value'
        >>> x.set_location('b')
        Traceback (most recent call last):
        ...
        KeyError: no key matching 'b' was found
        >>> x.close()                
        
        """
        (key, pickled) = self.dict.set_location(key)
        return self._unpickle(key, pickled)

    def next(self):
        """Sets the cursor to the next record in the database and returns
        a (key, value) for the record.

        >>> x = open('shelve.db', flag='nf')
        >>> x['b'] = (2, 'baz')
        >>> x['a'] = (1, 'bar')
        >>> x.first()
        ('a', (1, 'bar'))
        >>> x.next()
        ('b', (2, 'baz'))
        >>> x.close()            

        A KeyError is raised when the end of the table is reached or if 
        the table is empty.
                
        >>> x = open('shelve.db', flag='nf')
        >>> x.next()
        Traceback (most recent call last):
        ...
        KeyError: end of database
        >>> x.close()                            
    
        """    
        (key, pickled) = self.dict.next()
        return self._unpickle(key, pickled)

    def previous(self):
        """Sets the cursor to the previous record in the database and returns
        a (key, value) for the record.

        >>> x = open('shelve.db', flag='nf')
        >>> x['b'] = (2, 'baz')
        >>> x['a'] = (1, 'bar')
        >>> x.last()
        ('b', (2, 'baz'))
        >>> x.previous()
        ('a', (1, 'bar'))
        >>> x.close()            

        A KeyError is raised when the end of the table is reached or if 
        the table is empty.
                
        >>> x = open('shelve.db', flag='nf')
        >>> x.previous()
        Traceback (most recent call last):
        ...
        KeyError: end of database
        >>> x.close()                            
    
        """    
        (key, pickled) = self.dict.previous()
        return self._unpickle(key, pickled)

    def first(self):
        """Sets the cursor to the first record in the database and returns
        a (key, value) for the record.
        
        >>> x = open('wdbtest.db', flag='nf')
        >>> x['b'] = (2, 'baz')
        >>> x['a'] = (1, 'bar')
        >>> x.first()
        ('a', (1, 'bar'))
        >>> x.close()            
        
        If the database is empty a KeyError is raised.

        >>> x = open('wdbtest.db', flag='nf')
        >>> x.first()
        Traceback (most recent call last):
        ...
        KeyError: database is empty
        >>> x.close()            
        
        """    
        (key, pickled) = self.dict.first()
        return self._unpickle(key, pickled)

    def last(self):
        """Sets the cursor to the last record in the database and returns
        a (key, value) for the record.
        
        >>> x = open('wdbtest.db', flag='nf')
        >>> x['b'] = (2, 'baz')
        >>> x['a'] = (1, 'bar')
        >>> x.last()
        ('b', (2, 'baz'))
        >>> x.close()            
        
        If the database is empty a KeyError is raised.

        >>> x = open('wdbtest.db', flag='nf')
        >>> x.last()
        Traceback (most recent call last):
        ...
        KeyError: database is empty
        >>> x.close()            
        
        """    
        (key, pickled) = self.dict.last()
        return self._unpickle(key, pickled)
        
    def _unpickle(self, key, pickled):
        f = StringIO.StringIO(pickled)
        return (key, pickle.Unpickler(f).load())        

#-----------------------------------------------------------------------
def open(filename, flag='c', protocol=None, writeback=False):
#-----------------------------------------------------------------------
    """Open a shelf (persistent dictionary) for reading and writing.

    The filename parameter is the name of the underlying database. The optional
    flag parameter has the same interpretation as the flag parameter of
    esedb.open(). The optional protocol parameter specifies the version
    of the pickle protocol (0, 1, or 2).

    """    
    return EseDbShelf(filename, flag, protocol, writeback)
        
if __name__ == '__main__':
    import doctest
    doctest.testmod()
        