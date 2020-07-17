using System;
using System.Threading;

namespace Raven.Client.Util
{
    internal class EasyReaderWriterLock
    {
        readonly ReaderWriterLockSlim _inner = new ReaderWriterLockSlim();

        public bool IsReadLockHeld => _inner.IsReadLockHeld;

        public bool IsWriteLockHeld => _inner.IsWriteLockHeld;

        public IDisposable EnterReadLock()
        {
            if (_inner.IsReadLockHeld || _inner.IsWriteLockHeld)
                return new DisposableAction(() => { });

            _inner.EnterReadLock();
            return new DisposableAction(_inner.ExitReadLock);
        }

        public IDisposable EnterWriteLock()
        {
            if (_inner.IsWriteLockHeld)
                return new DisposableAction(() => { });

            _inner.EnterWriteLock();
            return new DisposableAction(_inner.ExitWriteLock);
        }

        public IDisposable TryEnterWriteLock(TimeSpan ts)
        {
            if (_inner.IsWriteLockHeld)
                return new DisposableAction(() => { });

            if (_inner.TryEnterWriteLock(ts) == false)
                return null;

            return new DisposableAction(_inner.ExitWriteLock);
        }
    }
}
