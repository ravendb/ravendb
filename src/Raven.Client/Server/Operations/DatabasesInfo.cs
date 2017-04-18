using System;
using System.Collections.Generic;
using Raven.Client.Documents.Indexes;
using Raven.Client.Util;

namespace Raven.Client.Server.Operations
{
    public class DatabasesInfo
    {
        public List<DatabaseInfo> Databases { get; set; }
    }

    public class BackupInfo
    {
        public DateTime LastIncrementalBackup { get; set; }

        public DateTime LastFullBackup { get; set; }

        public TimeSpan IncrementalBackupInterval { get; set; }

        public TimeSpan FullBackupInterval { get; set; }

    }

    public class DatabaseInfo
    {
        public string Name { get; set; }
        public bool Disabled { get; set; }

        public string LoadError { get; set; }

        public Size TotalSize { get; set; }

        public bool IsAdmin { get; set; }

        public int? IndexingErrors { get; set; }

        public int? Alerts { get; set; }

        public TimeSpan? UpTime { get; set; }

        public BackupInfo BackupInfo { get; set; }

        public List<string> Bundles { get; set; }

        public bool RejectClients { get; set; }

        public IndexRunningStatus IndexingStatus { get; set; }

        public int? DocumentsCount { get; set; }

        public int? IndexesCount { get; set; }
    }

}