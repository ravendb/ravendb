using System;
using System.Threading;

namespace Sparrow.Server.Utils
{
    /// <summary>
    /// This class allow us to perform disposal operations without running
    /// into concurrency issues with calling code
    /// </summary>
    public class DisposeLock
    {
        private bool _disposed;
        private readonly string _name;
        private readonly ReaderWriterLockSlim _lock;

        public DisposeLock(string name)
        {
            _name = name;
            _lock = new ReaderWriterLockSlim();
            _disposed = false;
        }

        public struct ReadRelease : IDisposable
        {
            private DisposeLock _parent;

            public ReadRelease(DisposeLock parent)
            {
                _parent = parent;
            }

            public void Dispose()
            {
                _parent._lock.ExitReadLock();
            }
        }

        public struct WriteRelease : IDisposable
        {
            private DisposeLock _parent;

            public WriteRelease(DisposeLock parent)
            {
                _parent = parent;
            }

            public void Dispose()
            {
                _parent._lock.ExitWriteLock();
            }
        }

        public ReadRelease EnsureNotDisposed()
        {
            _lock.EnterReadLock();

            if (_disposed)
            {
                _lock.ExitReadLock();
                ThrowDisposed();
            }

            return new ReadRelease(this);
        }

        public WriteRelease StartDisposing()
        {
            _lock.EnterWriteLock();
            _disposed = true;
            return new WriteRelease(this);
        }

        private void ThrowDisposed()
        {
            throw new LockAlreadyDisposedException(_name);
        }
    }

    public class LockAlreadyDisposedException : ObjectDisposedException
    {
        public LockAlreadyDisposedException(string message) : base(message)
        {
        }

        public LockAlreadyDisposedException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}
