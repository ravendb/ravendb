using System;
using System.IO;
using Raven.Database;

namespace Raven.Storage.Managed.Impl
{
    public class ReadOnlyFileBasedPersistentSource : IPersistentSource
    {
        private readonly string basePath;
        private readonly string prefix;
        private readonly string dataPath;
        private readonly string logPath;

        private FileStream data;
        private FileStream log;

        public bool CreatedNew { get; set; }

        public ReadOnlyFileBasedPersistentSource(string basePath, string prefix)
        {
            SyncLock = new object();
            this.basePath = basePath;
            this.prefix = prefix;
            dataPath = Path.Combine(basePath, prefix + ".data");
            logPath = Path.Combine(basePath, prefix + ".log");

            OpenFiles();
        }

        private void OpenFiles()
        {
            log = new FileStream(logPath, FileMode.OpenOrCreate, FileAccess.Read, FileShare.ReadWrite, 4096, FileOptions.SequentialScan);
            data = new FileStream(dataPath, FileMode.OpenOrCreate, FileAccess.Read, FileShare.ReadWrite, 4096, FileOptions.RandomAccess);
        }

        #region IPersistentSource Members

        public object SyncLock
        {
            get;
            private set;
        }

        public Stream Data
        {
            get { return data; }
        }

        public Stream Log
        {
            get { return log; }

        }

        public void ReplaceAtomically(Stream newData, Stream newLog)
        {
           throw new NotSupportedException();
        }

        public Stream CreateTemporaryStream()
        {
            throw new NotSupportedException();
        }

        public void FlushData()
        {
            throw new NotSupportedException();
        }

        public void FlushLog()
        {
            throw new NotSupportedException();
        }

        public RemoteManagedStorageState CreateRemoteAppDomainState()
        {
            return new RemoteManagedStorageState
            {
                Path = basePath,
                Prefix = prefix
            };
        }

        #endregion

        public void Dispose()
        {
            data.Dispose();
            log.Dispose();
        }
    }
}