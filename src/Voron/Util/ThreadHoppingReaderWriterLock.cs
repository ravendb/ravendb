using System;
using System.Threading;

namespace Voron.Util
{

    /// <summary>
    /// Assumptions:
    /// * Only single writer 
    /// * Write operations can be very long ( involves I/O )
    /// * Many readers
    /// * Writer waiting should stop additional readers from entering
    /// * Entering & exiting the read lock can happen in different threads (no thread affinity)
    /// * Write lock is always on the same thread
    /// </summary>
    public class ThreadHoppingReaderWriterLock
    {
        private int _waiters;
        private SpinLock _readWaitLock = new SpinLock();
        private readonly ManualResetEventSlim _readerWait = new ManualResetEventSlim(false);
        private readonly AutoResetEvent _writerWait = new AutoResetEvent(false);

        private readonly ThreadLocal<int> _localThreadIdCache =
            new ThreadLocal<int>(() => Thread.CurrentThread.ManagedThreadId);
        private int _writeLockOwnerThreadId;

        private const uint ReaderMask = 0x00FFFFFF;
        private const int WriterMarker = 0x01000000;

        public bool WriteLockRequested => ((uint) Volatile.Read(ref _waiters) & ~ReaderMask) != 0;

        public void EnterWriteLock()
        {
            Interlocked.Add(ref _waiters, WriterMarker); // delcare intent to write

            while (Interlocked.CompareExchange(ref _writeLockOwnerThreadId, _localThreadIdCache.Value, 0) != 0) // try take ownership on lock
            {
                // we have readers, so we have to wait on them :-(
                _writerWait.WaitOne();
            }
        }


        public bool IsWriteLockHeld => _localThreadIdCache.Value == Volatile.Read(ref _writeLockOwnerThreadId);

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

                    if (_readerWait.IsSet)
                        _readerWait.Reset();

                    if (TryEnterReadLockCore())
                        return true;
                }
                finally
                {
                    if (lockTaken)
                        _readWaitLock.Exit(false);
                }

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

        private static void ThrowTooManyReaders(ulong waiters)
        {
            throw new InvalidOperationException(
                $"Too many readers, we got {waiters} readers, possible read lock leak");
        }

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

    }
}