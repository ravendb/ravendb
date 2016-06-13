using System;

namespace Raven.Client.Data
{
    public class PeriodicExportStatus
    {
        public DateTime LastBackup { get; set; }
        public DateTime LastFullBackup { get; set; }

        public long? LastDocsEtag { get; set; }
    }
}