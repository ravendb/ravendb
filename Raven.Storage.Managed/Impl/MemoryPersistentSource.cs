using System;
using System.IO;

namespace Raven.Storage.Managed.Impl
{
    public class MemoryPersistentSource : IPersistentSource
    {
        public MemoryPersistentSource()
        {
            SyncLock = new object();
            Data = new MemoryStream();
            Log = new MemoryStream();
        }

        public MemoryPersistentSource(byte[] data, byte[] log)
        {
            SyncLock = new object();
            Data = new MemoryStream(data);
            Log = new MemoryStream(log);
        }

        public object SyncLock
        {
            get;
            private set;
        }

        public Stream Data
        {
            get;
            set;
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

        public void ReplaceAtomically(Stream data, Stream log)
        {
            Data = data;
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
                Data = ((MemoryStream)Data).ToArray(),
                Log = ((MemoryStream)Log).ToArray(),
            };
        }

        public void Dispose()
        {
        }
    }
}