using System;
using System.Collections.Generic;
using System.IO;

namespace Raven.Munin
{
    public class MemoryPersistentSource : IPersistentSource
    {
        public MemoryPersistentSource()
        {
            SyncLock = new object();
            Log = new MemoryStream();
        }

        public T Read<T>(Func<Stream, T> readOnlyAction)
        {
            lock (SyncLock)
                return readOnlyAction(Log);
        }


        public IEnumerable<T> Read<T>(Func<Stream, IEnumerable<T>> readOnlyAction)
        {
            lock (SyncLock)
            {
                foreach (var item in readOnlyAction(Log))
                {
                    yield return item;
                }
            }
        }

        public void Write(Action<Stream> readWriteAction)
        {
            lock (SyncLock)
                readWriteAction(Log);
        }


        public MemoryPersistentSource(byte[] log)
        {
            SyncLock = new object();
            Log = new MemoryStream(log);
        }

        private object SyncLock
        {
            get; set;
        }

        private Stream Log
        {
            get;
            set;
        }

        public bool CreatedNew
        {
            get { return true; }
        }

        public void ReplaceAtomically(Stream log)
        {
            Log = log;
        }

        public Stream CreateTemporaryStream()
        {
            return new MemoryStream();
        }

        public void FlushData()
        {
        }

        public void FlushLog()
        {
        }

        public RemoteManagedStorageState CreateRemoteAppDomainState()
        {
            return new RemoteManagedStorageState
            {
                Log = ((MemoryStream)Log).ToArray(),
            };
        }

        public void Dispose()
        {
        }
    }
}