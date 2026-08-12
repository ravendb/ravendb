using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;

namespace Voron.Util
{

    /// <summary>
    /// Do not use this type in new code. The name says reader/writer lock, and the behaviour is
    /// not one. It is kept because the journal flush depends on the behaviour described below.
    /// The transaction lock rework in 8.0 replaces it, and it is removed there.
    ///
    /// A gate that blocks new readers while a writer is active. This is not a mutual-exclusion
    /// reader/writer lock: EnterWriteLock returns without waiting for readers that are already in.
    ///
    /// The read side and the write side have different thread rules. Only the read side can
    /// move between threads:
    /// * Read side: the lock counts the readers. It does not record which threads they are.
    ///   A read lock can be entered on one thread and exited on a different thread, for
    ///   example before and after an await.
    /// * Write side: the writer stays on one thread. The lock records the thread id of the
    ///   writer, IsWriteLockHeld is true only on that thread, and ExitWriteLock must run on
    ///   the thread that called EnterWriteLock, or it throws. Caution: an await between the
    ///   two calls can move the code to a different thread. A writer that must move between
    ///   threads needs a different ownership token than a thread id.
    ///
    /// Rules for callers:
    /// * Only one writer can be active at a time. EnterWriteLock waits for the active writer
    ///   to exit, and throws a TimeoutException when the timeout expires.
    /// * A thread must not call EnterWriteLock while it holds the write lock. The call throws.
    ///   This check reads the thread id, so it can only see a caller that stayed on the
    ///   entering thread. The timeout is what bounds every other case.
    /// * There is no drain. EnterWriteLock does not wait for the readers that are already
    ///   inside, and it does not stop them. It blocks new readers and returns immediately,
    ///   while the earlier readers keep running. Code that runs under the write lock runs
    ///   at the same time as those readers and must be safe to do so.
    /// * Each successful TryEnterReadLock must be paired with exactly one ExitReadLock.
    ///   The number of concurrent readers must stay below 2^23.
    /// * Where writer/reader mutual exclusion is necessary, callers must supply it with a
    ///   different mechanism.
    /// </summary>
    [Obsolete("Do not use in new code. The journal flush depends on this type, it is replaced by the transaction lock rework in 8.0.")]
    public sealed class ThreadHoppingReaderWriterLock
    {
        private const uint ReaderMask = 0x00FFFFFF;
        private const int WriterMarker = 0x01000000;
        private int _waiters;

        private SpinLock _readWaitLock = new SpinLock();
        private readonly ManualResetEventSlim _readerWait = new ManualResetEventSlim(false);
        private readonly AutoResetEvent _writerWait = new AutoResetEvent(false);
        private int _writeLockOwnerThreadId;


        public void EnterWriteLock()
        {
            var currentWaiters = Interlocked.Add(ref _waiters, WriterMarker);

            int managedThreadId = Thread.CurrentThread.ManagedThreadId;
            // try take ownership on lock
            var currentLock = Interlocked.CompareExchange(ref _writeLockOwnerThreadId, managedThreadId, 0);
            while (
                ( currentWaiters & ReaderMask ) == 0 &&
                currentLock != 0 
                )
            {
                // we have readers, so we have to wait on them :-(
                _writerWait.WaitOne();
                currentWaiters = Volatile.Read(ref _waiters);
                currentLock = Interlocked.CompareExchange(ref _writeLockOwnerThreadId, managedThreadId, 0);
            }
        }


        public bool IsWriteLockHeld => Thread.CurrentThread.ManagedThreadId == Volatile.Read(ref _writeLockOwnerThreadId);

        public void ExitWriteLock()
        {
            if (IsWriteLockHeld == false)
                ThrowInvalidWriteLockRelease();

            Interlocked.Add(ref _waiters, -WriterMarker); // remove the write marker for this lock
            Interlocked.Exchange(ref _writeLockOwnerThreadId, 0); // remove ownering of lock
            _readerWait.Set();
            _writerWait.Set();
        }


        public void ExitReadLock()
        {
            var waiters = Interlocked.Decrement(ref _waiters);
            if ((waiters & ~ReaderMask) != 0)
            {
                _writerWait.Set();
            }
        }

        public bool TryEnterReadLock(TimeSpan timeout)
        {
            return TryEnterReadLock((int)timeout.TotalMilliseconds);
        }

        public bool TryEnterReadLock(int timeout)
        {
            if (TryEnterReadLockCore())
                return true;

            return TryEnterReadLockSlow(timeout);
        }

        private bool TryEnterReadLockSlow(int timeout)
        {
            var tracker = new TimeoutTracker(timeout);
            while (tracker.IsExpired == false)
            {
                bool lockTaken = false;
                _readWaitLock.TryEnter(tracker.RemainingMilliseconds, ref lockTaken);

                try
                {
                    if (lockTaken == false)
                        return false;

                    ForTestingPurposes?.BeforeResetOfReaderWait?.Invoke();

                    if (_readerWait.IsSet)
                        _readerWait.Reset();

                    if (TryEnterReadLockCore())
                    {
                        // we got the reader lock, that means that no writer can acquire it.
                        // since there might be other readers that are waiting on the _readerWait,
                        // we need to signal them (and we are the only ones that can do that).
                        _readerWait.Set();
                        return true;
                    }
                }
                finally
                {
                    if (lockTaken)
                        _readWaitLock.Exit(false);
                }

                ForTestingPurposes?.BeforeReaderWriterWait?.Invoke();

                _readerWait.Wait(tracker.RemainingMilliseconds);

                if (TryEnterReadLockCore())
                    return true;
            }

            return false;
        }

        private bool TryEnterReadLockCore()
        {
            var waiters = (uint)Interlocked.Increment(ref _waiters);
            if ((waiters & ~ReaderMask) != 0) // there is a writer
            {
                ExitReadLock();
                return false;
            }

            if (waiters > ReaderMask / 2)
            {
                ExitReadLock();
                ThrowTooManyReaders(waiters);
            }

            return true;
        }

        [DoesNotReturn]
        private static void ThrowTooManyReaders(ulong waiters)
        {
            throw new InvalidOperationException(
                $"Too many readers, we got {waiters} readers, possible read lock leak");
        }

        [DoesNotReturn]
        private static void ThrowInvalidWriteLockRelease()
        {
            throw new InvalidOperationException("Attempt to release write lock that isn't being held");
        }

        private struct TimeoutTracker
        {
            private readonly int _total;
            private readonly int _start;

            public TimeoutTracker(int millisecondsTimeout)
            {
                if (millisecondsTimeout < -1)
                    throw new ArgumentOutOfRangeException(nameof(millisecondsTimeout));
                _total = millisecondsTimeout;
                if (_total != -1 && _total != 0)
                    _start = Environment.TickCount;
                else
                    _start = 0;
            }

            public int RemainingMilliseconds
            {
                get
                {
                    if (_total == -1 || _total == 0)
                        return _total;

                    int elapsed = Environment.TickCount - _start;
                    // elapsed may be negative if TickCount has overflowed by 2^31 milliseconds.
                    if (elapsed < 0 || elapsed >= _total)
                        return 0;

                    return _total - elapsed;
                }
            }

            public bool IsExpired => RemainingMilliseconds == 0;
        }

        internal TestingStuff ForTestingPurposes;

        internal TestingStuff ForTestingPurposesOnly()
        {
            if (ForTestingPurposes != null)
                return ForTestingPurposes;

            return ForTestingPurposes = new TestingStuff();
        }

        internal class TestingStuff
        {
            internal Action BeforeResetOfReaderWait;
            internal Action BeforeReaderWriterWait;
        }
    }
}
