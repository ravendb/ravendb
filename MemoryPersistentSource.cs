using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading;

namespace Raven.Munin
{
    public class MemoryPersistentSource : IPersistentSource
    {
        private readonly ReaderWriterLockSlim locker = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);
        private readonly StreamsPool pool;

        public MemoryPersistentSource()
        {
            Log = new MemoryStream();
            pool = new StreamsPool(CreateClonedStreamForReadOnlyPurposes);
        }

        private Stream CreateClonedStreamForReadOnlyPurposes()
        {
            return new MemoryStream(Log.GetBuffer(), 0, (int) Log.Length, false);
        }

        public MemoryPersistentSource(byte[] log)
        {
            Log = new MemoryStream(log);
            pool = new StreamsPool(CreateClonedStreamForReadOnlyPurposes);
        }

        private MemoryStream Log { get; set; }

        #region IPersistentSource Members

        public T Read<T>(Func<Stream, T> readOnlyAction)
        {
            bool lockAlreadyHeld = locker.IsReadLockHeld || locker.IsWriteLockHeld;
            if (lockAlreadyHeld == false)
                locker.EnterReadLock();
            try
            {
                Stream stream;
                using(pool.Use(out stream))
                    return readOnlyAction(stream);
            }
            finally
            {
                if (lockAlreadyHeld == false)
                    locker.ExitReadLock();
            }
        }

        public IEnumerable<T> Read<T>(Func<Stream, IEnumerable<T>> readOnlyAction)
        {
            bool lockAlreadyHeld = locker.IsReadLockHeld || locker.IsWriteLockHeld;
            if (lockAlreadyHeld == false)
                locker.EnterReadLock();
            try
            {
                Stream stream;
                using (pool.Use(out stream))
                {
                    foreach (T item in readOnlyAction(Log))
                    {
                        yield return item;
                    }
                }
            }
            finally
            {
                if (lockAlreadyHeld == false)
                    locker.ExitReadLock();
            }
        }

        public void Write(Action<Stream> readWriteAction)
        {
            bool lockAlreadyHeld = locker.IsWriteLockHeld;
            if (lockAlreadyHeld == false)
                locker.EnterWriteLock();
            try
            {
                readWriteAction(Log);
            }
            finally
            {
                if (lockAlreadyHeld == false)
                {
                    pool.Clear();
                    locker.ExitWriteLock();
                }
            }
        }

        public bool CreatedNew
        {
            get { return true; }
        }

        public void ReplaceAtomically(Stream log)
        {
            Log = (MemoryStream)log;
        }

        public Stream CreateTemporaryStream()
        {
            return new MemoryStream();
        }

        public void FlushLog()
        {
        }

        public RemoteManagedStorageState CreateRemoteAppDomainState()
        {
            return new RemoteManagedStorageState
            {
                Log = ((MemoryStream) Log).ToArray(),
            };
        }

        public void Dispose()
        {
        }

        #endregion

        public void FlushData()
        {
        }
    }
}