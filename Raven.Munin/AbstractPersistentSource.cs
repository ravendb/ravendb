using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;

namespace Raven.Munin
{
    public abstract class AbstractPersistentSource : IPersistentSource
    {
        private readonly ReaderWriterLockSlim locker = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);
        private readonly StreamsPool pool;

        protected AbstractPersistentSource()
        {
            pool = new StreamsPool(CreateClonedStreamForReadOnlyPurposes);
        }

        public bool CreatedNew
        {
            get; protected set;
        }

        protected abstract Stream CreateClonedStreamForReadOnlyPurposes();

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
                    foreach (T item in readOnlyAction(stream))
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
                if(lockAlreadyHeld == false)
                    // we need to clear the pool (and dispose all outstanding references, before we execute the write call
                    pool.Clear();

                readWriteAction(Log);
            }
            finally
            {
                if (lockAlreadyHeld == false)
                    locker.ExitWriteLock();
            }
        }

        protected abstract Stream Log { get; }

        public abstract void ReplaceAtomically(Stream log);
        public abstract Stream CreateTemporaryStream();
        public abstract void FlushLog();
        public abstract RemoteManagedStorageState CreateRemoteAppDomainState();

        public void ClearPool()
        {
            pool.Clear();
        }

        public virtual void Dispose()
        {
            pool.Clear();
        }
    }
}