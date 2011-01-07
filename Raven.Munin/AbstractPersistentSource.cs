//-----------------------------------------------------------------------
// <copyright file="AbstractPersistentSource.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
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
            get;
            protected set;
        }

        public IList<PersistentDictionaryState> DictionariesStates
        {
            get { return currentStates.Value ?? globalStates; }
        }

        protected abstract Stream CreateClonedStreamForReadOnlyPurposes();

        public T Read<T>(Func<Stream, T> readOnlyAction)
        {
            var needUpdating = currentStates.Value == null;
            if (needUpdating)
                currentStates.Value = globalStates;
            try
            {
                Stream stream;
                using (pool.Use(out stream))
                    return readOnlyAction(stream);
            }
            finally
            {
                if (needUpdating)
                    currentStates.Value = null;
            }
        }

        public T Read<T>(Func<T> readOnlyAction)
        {
            var needUpdating = currentStates.Value == null;
            if (needUpdating)
                currentStates.Value = globalStates;
            try
            {
                return readOnlyAction();
            }
            finally
            {
                if (needUpdating)
                    currentStates.Value = null;
            }
        }

        public IEnumerable<T> Read<T>(Func<IEnumerable<T>> readOnlyAction)
        {
            var needUpdating = currentStates.Value == null;
            if (needUpdating)
                currentStates.Value = globalStates;
            try
            {
                foreach (T item in readOnlyAction())
                {
                    yield return item;
                }
            }
            finally
            {
                if (needUpdating)
                    currentStates.Value = null;
            }
        }

        public void Write(Action<Stream> readWriteAction)
        {
            lock (this)
            {
                try
                {
                    currentStates.Value = new List<PersistentDictionaryState>(
                        globalStates.Select(x => new PersistentDictionaryState(x.Comparer)
                        {
                            KeyToFilePositionInFiles = x.KeyToFilePositionInFiles,
                            SecondaryIndicesState = x.SecondaryIndicesState
                        }));

                    readWriteAction(Log);
                }
                finally
                {
                    pool.Clear();
                    Interlocked.Exchange(ref globalStates, currentStates.Value);
                    currentStates.Value = null;
                }
            }
        }

        protected abstract Stream Log { get; }

        public abstract void ReplaceAtomically(Stream newLog);
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