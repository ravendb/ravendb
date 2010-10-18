using System;
using System.Collections.Generic;
using System.IO;

namespace Raven.Munin
{
    public class FileBasedPersistentSource : IPersistentSource
    {
        private readonly string basePath;
        private readonly string prefix;
        private readonly bool writeThrough;
        private readonly string logPath;

        private FileStream log;

        public T Read<T>(Func<Stream, T> readOnlyAction)
        {
            lock(SyncLock)
                return readOnlyAction(log);
        }

        public IEnumerable<T> Read<T>(Func<Stream, IEnumerable<T>> readOnlyAction)
        {
            lock(SyncLock)
            {
                foreach (var item in readOnlyAction(log))
                {
                    yield return item;
                }
            }
        }

        public void Write(Action<Stream> readWriteAction)
        {
            lock (SyncLock)
                readWriteAction(log);
        }

        public bool CreatedNew { get; set; }

        public FileBasedPersistentSource(string basePath, string prefix, bool writeThrough)
        {
            SyncLock = new object();
            this.basePath = basePath;
            this.prefix = prefix;
            this.writeThrough = writeThrough;
            logPath = Path.Combine(basePath, prefix + ".log");


            RecoverFromFailedRename(logPath);

            CreatedNew = File.Exists(logPath) == false;

            OpenFiles();
        }

        private void OpenFiles()
        {
            log = new FileStream(logPath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read, 4096,
                                 writeThrough ? FileOptions.WriteThrough | FileOptions.SequentialScan : FileOptions.SequentialScan
                );
        }

        #region IPersistentSource Members

        private object SyncLock
        {
            get; set;
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