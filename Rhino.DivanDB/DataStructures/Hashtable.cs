using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
using System.Linq;

namespace Rhino.DivanDB.DataStructures
{
    public class Hashtable<TKey,TVal> : IEnumerable<KeyValuePair<TKey,TVal>>
    {
        private readonly ReaderWriterLockSlim readerWriterLockSlim = new ReaderWriterLockSlim();
        private readonly Dictionary<TKey,TVal> dictionary = new Dictionary<TKey, TVal>();

        public class Reader
        {
            protected Hashtable<TKey, TVal> parent;

            public Reader(Hashtable<TKey, TVal> parent)
            {
                this.parent = parent;
            }

            public bool TryGetValue(TKey key, out TVal val)
            {
                return parent.dictionary.TryGetValue(key, out val);
            }
        }

        public class Writer : Reader
        {
            public Writer(Hashtable<TKey, TVal> parent) : base(parent)
            {
            }

            public void Add(TKey key, TVal val)
            {
                parent.dictionary[key] = val;
            }

            public bool Remove(TKey key)
            {
                return parent.dictionary.Remove(key);
            }
        }

        public void Write(Action<Writer> action)
        {
            readerWriterLockSlim.EnterWriteLock();
            try
            {
                action(new Writer(this));
            }
            finally
            {
                readerWriterLockSlim.ExitWriteLock();
            }
        }

        public void Read(Action<Reader> read)
        {
            readerWriterLockSlim.EnterReadLock();
            try
            {
                read(new Reader(this));
            }
            finally
            {
                readerWriterLockSlim.ExitReadLock();
            }
        }

        public IEnumerator<KeyValuePair<TKey, TVal>> GetEnumerator()
        {
            readerWriterLockSlim.EnterReadLock();
            try
            {
                return dictionary
                    .ToList()
                    .GetEnumerator();
            }
            finally
            {
                readerWriterLockSlim.ExitReadLock();
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}