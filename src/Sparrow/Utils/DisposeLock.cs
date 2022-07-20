using System;
using System.Threading;
using Nito.AsyncEx;

namespace Sparrow.Utils
{
    /// <summary>
    /// This class allow us to perform disposal operations without running
    /// into concurrency issues with calling code
    /// </summary>
    public class DisposeLock
    {
        private readonly string _name;
        private readonly AsyncReaderWriterLock _lock;
        private readonly CancellationTokenSource _cts;

        public DisposeLock(string name)
        {
            _name = name;
            _cts = new CancellationTokenSource();
            _lock = new AsyncReaderWriterLock();
        }

        public IDisposable EnsureNotDisposed()
        {
            IDisposable disposable = null;
            try
            {
                disposable = _lock.ReaderLock(_cts.Token);
            }
            catch
            {
               // ignore
            }
            
            if (disposable == null || 
                _cts.IsCancellationRequested)
            {
                disposable?.Dispose();
                ThrowDisposed();
            }

            return disposable;
        }

        public IDisposable StartDisposing()
        {
            var disposable = _lock.WriterLock(_cts.Token);
            _cts.Cancel();
            return disposable;
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
