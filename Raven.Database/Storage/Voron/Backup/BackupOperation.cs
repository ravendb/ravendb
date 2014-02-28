using Raven.Abstractions.Data;
using System;
using System.IO;
using Voron;
using Voron.Impl.Backup;

namespace Raven.Database.Storage.Voron.Backup
{
    public class BackupOperation : BaseBackupOperation
    {
        private readonly StorageEnvironment env;

        public BackupOperation(DocumentDatabase database, string backupSourceDirectory,
                               string backupDestinationDirectory, StorageEnvironment env, bool incrementalBackup,
                               DatabaseDocument databaseDocument)
            : base(database, backupSourceDirectory, backupDestinationDirectory, incrementalBackup, databaseDocument)
        {
            if (env == null) throw new ArgumentNullException("env");

            this.env = env;
        }

        protected override bool BackupAlreadyExists
        {
            get { return Directory.Exists(backupDestinationDirectory) && File.Exists(Path.Combine(backupDestinationDirectory.Trim(), BackupMethods.Filename)); }
        }

        protected override void ExecuteBackup(string backupPath, bool isIncrementalBackup)
        {
            if (string.IsNullOrWhiteSpace(backupPath)) throw new ArgumentNullException("backupPath");

            if (isIncrementalBackup)
                BackupMethods.Incremental.ToFile(env, Path.Combine(backupPath, BackupMethods.Filename));
            else
                BackupMethods.Full.ToFile(env, Path.Combine(backupPath, BackupMethods.Filename));
        }
    }
}
