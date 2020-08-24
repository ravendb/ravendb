using System;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Config.Settings;

namespace Raven.Server.Documents.PeriodicBackup
{
    public class BackupParameters
    {
        public PeriodicBackupStatus BackupStatus { get; set; }
        public long OperationId { get; set; }
        public RetentionPolicy RetentionPolicy { get; set; }
        public DateTime StartTimeUtc { get; set; }

        public bool IsOneTimeBackup { get; set; }
        public bool IsFullBackup { get; set; }
        public bool BackupToLocalFolder { get; set; }

        public PathSetting TempBackupPath { get; set; }
        public string Name { get; set; }
    }
}
