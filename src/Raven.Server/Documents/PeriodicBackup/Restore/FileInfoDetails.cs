using System;

namespace Raven.Server.Documents.PeriodicBackup.Restore
{
    public class FileInfoDetails
    {
        public string FullPath { get; set; }

        public DateTime LastModified { get; set; }
    }
}