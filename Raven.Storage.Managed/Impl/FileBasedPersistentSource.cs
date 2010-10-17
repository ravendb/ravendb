using System;
using System.IO;
using Raven.Database;

namespace Raven.Storage.Managed.Impl
{
    public class FileBasedPersistentSource : IPersistentSource
    {
        private readonly string basePath;
        private readonly string prefix;
        private readonly TransactionMode transactionMode;
        private readonly string logPath;

        private FileStream log;

        public bool CreatedNew { get; set; }

        public FileBasedPersistentSource(string basePath, string prefix, TransactionMode transactionMode)
        {
            SyncLock = new object();
            this.basePath = basePath;
            this.prefix = prefix;
            this.transactionMode = transactionMode;
            logPath = Path.Combine(basePath, prefix + ".log");


            RecoverFromFailedRename(logPath);

            CreatedNew = File.Exists(logPath) == false;

            OpenFiles();
        }

        private void OpenFiles()
        {
            log = new FileStream(logPath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read, 4096,
                                 transactionMode == TransactionMode.Lazy ? FileOptions.SequentialScan : FileOptions.WriteThrough| FileOptions.SequentialScan
                );
        }

        #region IPersistentSource Members

        public object SyncLock
        {
            get;
            private set;
        }

        public Stream Log
        {
            get { return log; }

        }

        public void ReplaceAtomically(Stream newLog)
        {
            var newLogStream = ((FileStream)newLog);
            string logTempName = newLogStream.Name;
            newLogStream.Flush();
            newLogStream.Dispose();

            newLog.Dispose();

            log.Dispose();

            string renamedLogFile = logPath + ".rename_op";

            File.Move(logPath, renamedLogFile);

            File.Move(logTempName, logPath);

            File.Delete(renamedLogFile);

            OpenFiles();
        }

        public Stream CreateTemporaryStream()
        {
            string tempFile = Path.Combine(basePath, Path.GetFileName(Path.GetTempFileName()));
            return File.Open(tempFile, FileMode.Create, FileAccess.ReadWrite);
        }

        public void FlushLog()
        {
            log.Flush(true);
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

        private static void RecoverFromFailedRename(string file)
        {
            string renamedFile = file + ".rename_op";
            if (File.Exists(renamedFile) == false) // not in the middle of rename op, we are good
                return;

            if (File.Exists(file))
            // we successfully renamed the new file and crashed before we could remove the old copy
            {
                //just complete the op and we are good (committed)
                File.Delete(renamedFile);
            }
            else // we successfully renamed the old file and crashed before we could remove the new file
            {
                // just undo the op and we are good (rollback)
                File.Move(renamedFile, file);
            }
        }

        public void Dispose()
        {
            log.Dispose();
        }

        public void Delete()
        {
            File.Delete(logPath);
        }
    }
}