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
            _lock.EnterWriteLock();

            try
            {
                _internalList.Add(value);
            }
            finally
            {
                _lock.ExitWriteLock();
            }
        }

        public bool Remove(T value)
        {
            _lock.EnterWriteLock();

            try
            {
                return _internalList.Remove(value);
            }
            finally
            {
                _lock.ExitWriteLock();
            }
        }

        public bool Contains(T value)
        {
            _lock.EnterReadLock();

            try
            {
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

            _lock.EnterReadLock();

            try
            {
                list = new List<T>(_internalList);
            }
            finally
            {
                _lock.ExitReadLock();
            }

            return list.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}