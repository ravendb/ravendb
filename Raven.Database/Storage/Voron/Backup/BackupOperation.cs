using Raven.Abstractions.Data;
using System;
using System.IO;
using System.Threading;
using Voron;
using Voron.Impl.Backup;

namespace Raven.Database.Storage.Voron.Backup
{
    internal class BackupOperation : BaseBackupOperation, IDisposable
    {
        private readonly StorageEnvironment env;

        public BackupOperation(DocumentDatabase database, string backupSourceDirectory,
                               string backupDestinationDirectory, StorageEnvironment env, bool incrementalBackup,
                               DatabaseDocument databaseDocument, ResourceBackupState state, CancellationToken cancellationToken)
            : base(database, backupSourceDirectory, backupDestinationDirectory, incrementalBackup, databaseDocument, state, cancellationToken)
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
                BackupMethods.Incremental.ToFile(env, Path.Combine(backupPath, BackupMethods.Filename),
                    infoNotify: s => UpdateBackupStatus(s, null, BackupStatus.BackupMessageSeverity.Informational));
            else
                BackupMethods.Full.ToFile(env, Path.Combine(backupPath, BackupMethods.Filename),
                    infoNotify: s => UpdateBackupStatus(s, null, BackupStatus.BackupMessageSeverity.Informational));
        }

        public void Dispose()
        {
        }

        protected override bool CanPerformIncrementalBackup()
        {
            return true;
        }
    }
}

