using System;

namespace Raven.Server.Documents.PeriodicExport
{
    public class PeriodicExportStatus
    {
        public DateTime LastExportAt { get; set; }
        public DateTime LastFullExportAt { get; set; }

        public long? LastDocsEtag { get; set; }
    }
}