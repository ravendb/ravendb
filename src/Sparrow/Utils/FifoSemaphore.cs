using System;
using System.Collections.Generic;
using System.Threading;

namespace Sparrow.Utils
{
    // created based on https://github.com/pvgoran/dcutilities-codeplex-archive/blob/main/sourceCode/dcutilities/Bcl/Concurrency/FifoSemaphore.cs
    // added support for CancellationToken and use ManualResetEventSlim inside the waiter class instead of lock {} and Monitor.Wait / Monitor.Pulse

    /// <summary>
    /// A semaphore is a concurrency utility that contains a number of "tokens". Threads try to acquire
    /// (take) and release (put) these tokens into the semaphore. When a semaphore contains no tokens,
    /// threads that try to acquire a token will block until a token is released into the semaphore.
    /// This implementation guarantees the order in which client threads are able to acquire tokens
    /// will be in a first in first out manner.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This semaphore implementation guarantees that that threads will be served tokens on a first
    /// in first out basis. However, one should note that a thread is considered "in" not by when
    /// it calls any of the acquire methods, but by when it acquires the internal lock inside 
    /// any of the acquire methods. This is not normally an issue one need worry about.
    /// </para>
    /// <para>
    /// This class supports the safe use of interrupts. An interrupt that occurs within a method of
    /// this class results in the action performed by the method not occurring.
    /// </para>
    /// </remarks>
    internal class FifoSemaphore
    {
        internal readonly List<OneTimeWaiter> _waitQueue;

        protected readonly object _Lock;

        protected int _Tokens;

        public FifoSemaphore(int tokens)
        {
            if (tokens <= 0)
                throw new ArgumentException(nameof(tokens));

            _Tokens = tokens;
            _Lock = new object();
            _waitQueue = new List<OneTimeWaiter>();
        }

        public bool TryAcquire(TimeSpan timeout, CancellationToken token)
        {
            OneTimeWaiter waiter;

            lock (_Lock)
            {
                if (_Tokens > 0)
                {
                    _Tokens--;
                    return true;
                }

                token.ThrowIfCancellationRequested();

                _forTestingPurposes?.JustBeforeAddingToWaitQueue?.Invoke();

                waiter = new OneTimeWaiter();
                _waitQueue.Add(waiter);
            }

            using (waiter)
            {
                try
                {

                    return waiter.TryWait(timeout, token);
                }
                catch
                {
                    lock (_Lock)
                    {
                        _waitQueue.Remove(waiter);
                    }

                    throw;
                }
            }
        }

        public void Acquire(CancellationToken token)
        {
            var result = TryAcquire(Timeout.InfiniteTimeSpan, token);

            if (result == false)
                ThrowCouldNotAcquireLock();
        }

        private static void ThrowCouldNotAcquireLock()
        {
            throw new InvalidOperationException("Could not acquire the lock");
        }

        public void Release()
        {
            ReleaseMany(1);
        }

        public void ReleaseMany(int tokens)
        {
            lock (_Lock)
            {
                for (int i = 0; i < tokens; i++)
                {
                    if (_waitQueue.Count > 0)
                    {
                        var waiter = _waitQueue[0];

                        _waitQueue.RemoveAt(0);

                        waiter.Release();
                    }
                    else
                    {
                        //We've got no one waiting, so add a token
                        _Tokens++;
                    }
                }
            }
        }

        internal class OneTimeWaiter : IDisposable
        {
            private readonly ManualResetEventSlim _mre = new ManualResetEventSlim(false);
            
            public bool TryWait(TimeSpan timeout, CancellationToken token)
            {
                return _mre.Wait(timeout, token);
            }

            public void Release()
            {
                _mre.Set();
            }

            public void Dispose()
            {
                _mre?.Dispose();
            }
        }

        private TestingStuff _forTestingPurposes;

        internal TestingStuff ForTestingPurposesOnly()
        {
            if (_forTestingPurposes != null)
                return _forTestingPurposes;

            return _forTestingPurposes = new TestingStuff();
        }

        internal class TestingStuff
        {
            internal Action JustBeforeAddingToWaitQueue;

        }
    }
}
