using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Linq;
using Newtonsoft.Json.Linq;

namespace Raven.Munin
{
    public abstract class AbstractPersistentSource : IPersistentSource
    {
        private readonly ReaderWriterLockSlim locker = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);
        private readonly StreamsPool pool;
        private IList<PersistentDictionaryState> globalStates = new List<PersistentDictionaryState>();

        private readonly ThreadLocal<IList<PersistentDictionaryState>> currentStates =
            new ThreadLocal<IList<PersistentDictionaryState>>(() => null);


        protected AbstractPersistentSource()
        {
            pool = new StreamsPool(CreateClonedStreamForReadOnlyPurposes);
        }

        public bool CreatedNew
        {
            get; protected set;
        }

        public IList<PersistentDictionaryState> DictionariesStates
        {
            get { return currentStates.Value ?? globalStates; }
        }

        protected abstract Stream CreateClonedStreamForReadOnlyPurposes();

        public T Read<T>(Func<Stream, T> readOnlyAction)
        {
            bool lockAlreadyHeld = locker.IsReadLockHeld || locker.IsWriteLockHeld;
            if (lockAlreadyHeld == false)
                locker.EnterReadLock();
            var needUpdating = currentStates.Value == null;
            if (needUpdating)
                currentStates.Value = globalStates;
            try
            {
                Stream stream;
                using(pool.Use(out stream))
                    return readOnlyAction(stream);
            }
            finally
            {
                if (needUpdating)
                    currentStates.Value = null;
                if (lockAlreadyHeld == false)
                    locker.ExitReadLock();
            }
        }

        public IEnumerable<T> Read<T>(Func<Stream, IEnumerable<T>> readOnlyAction)
        {
            bool lockAlreadyHeld = locker.IsReadLockHeld || locker.IsWriteLockHeld;
            if (lockAlreadyHeld == false)
                locker.EnterReadLock();
            var needUpdating = currentStates.Value == null;
            if (needUpdating)
                currentStates.Value = globalStates;
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
                if (needUpdating)
                    currentStates.Value = null;
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

                if (lockAlreadyHeld == false)
                {
                    currentStates.Value = new List<PersistentDictionaryState>(
                        globalStates.Select(x => new PersistentDictionaryState(x.Comparer)
                        {
                            KeyToFilePositionInFiles =
                                                     new ConcurrentDictionary<JToken, PositionInFile>(
                                                     x.KeyToFilePositionInFiles, x.Comparer),
                            SecondaryIndices =
                                                     new List<SecondaryIndex>(
                                                     x.SecondaryIndices.Select(y => new SecondaryIndex(y)))
                        })
                        );
                }

                readWriteAction(Log);
            }
            finally
            {
                if (lockAlreadyHeld == false)
                {
                    pool.Clear();
                    Interlocked.Exchange(ref globalStates, currentStates.Value);
                    currentStates.Value = null;
                    locker.ExitWriteLock();
                }
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
            pool.Dispose();
        }
    }
}