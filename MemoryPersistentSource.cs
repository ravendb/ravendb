using System;
using System.IO;

namespace Raven.Storage.Managed.Impl
{
    public class MemoryPersistentSource : IPersistentSource
    {
        public MemoryPersistentSource()
        {
            SyncLock = new object();
            Log = new MemoryStream();
        }

        public MemoryPersistentSource(byte[] log)
        {
            SyncLock = new object();
            Log = new MemoryStream(log);
        }

        public object SyncLock
        {
            get;
            private set;
        }

        public Stream Log
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