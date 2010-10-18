using System;
using System.Collections.Generic;
using System.IO;

namespace Raven.Munin
{
    /// <summary>
    /// Simple read only version of te file based data.
    /// It is mostly meant for read only access from remote app domain.
    /// Because it is expected that this will only have very small usage scenario, it is not implemented in an efficent manner.
    /// </summary>
    public class ReadOnlyFileBasedPersistentSource : IPersistentSource
    {
        private readonly string basePath;
        private readonly string prefix;
        private readonly string logPath;

        private FileStream log;

        public T Read<T>(Func<Stream, T> readOnlyAction)
        {
            lock (SyncLock)
                return readOnlyAction(log);
        }

        public IEnumerable<T> Read<T>(Func<Stream, IEnumerable<T>> readOnlyAction)
        {
            lock (SyncLock)
            {
                foreach (var item in readOnlyAction(log))
                {
                    yield return item;
                }
            }
        }

        public void Write(Action<Stream> readWriteAction)
        {
            lock(SyncLock)
            {
                readWriteAction(log);
            }
        }

        public bool CreatedNew { get; set; }

        public ReadOnlyFileBasedPersistentSource(string basePath, string prefix)
        {
            SyncLock = new object();
            this.basePath = basePath;
            this.prefix = prefix;
            logPath = Path.Combine(basePath, prefix + ".log");

            OpenFiles();
        }

        private void OpenFiles()
        {
            log = new FileStream(logPath, FileMode.OpenOrCreate, FileAccess.Read, FileShare.ReadWrite, 4096, FileOptions.SequentialScan);
        }

        #region IPersistentSource Members

        private object SyncLock
        {
            get; set;
        }

        public void ReplaceAtomically(Stream newLog)
        {
           throw new NotSupportedException();
        }

        public Stream CreateTemporaryStream()
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
            log.Dispose();
        }
    }
}