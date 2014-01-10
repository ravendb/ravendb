namespace Voron.Util
{
    using System.Collections;
    using System.Collections.Generic;
    using System.Threading;

    public class ConcurrentList<T> : IEnumerable<T>
    {
        private readonly List<T> _internalList = new List<T>();

        private readonly ReaderWriterLockSlim _lock = new ReaderWriterLockSlim();

        public void Add(T value)
        {
            try
            {
                _lock.EnterWriteLock();
                _internalList.Add(value);
            }
            finally
            {
                _lock.ExitWriteLock();
            }
        }

        public bool  Remove(T value)
        {
            try
            {
                _lock.EnterWriteLock();
                return _internalList.Remove(value);
            }
            finally
            {
                _lock.ExitWriteLock();
            }
        }

        public bool Contains(T value)
        {
            try
            {
                _lock.EnterReadLock();
                return _internalList.Contains(value);
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }

        public IEnumerator<T> GetEnumerator()
        {
            List<T> list;

            try
            {
                _lock.EnterReadLock();
                list = new List<T>(_internalList);
            }
            finally
            {
                _lock.EnterReadLock();
            }

            return list.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}