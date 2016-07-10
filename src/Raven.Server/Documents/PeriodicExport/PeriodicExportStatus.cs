using System;

namespace Raven.Server.Documents.PeriodicExport
{
    public class PeriodicExportStatus
    {
        public long? LastExportAtTicks { get; set; }
        public long? LastFullExportAtTicks { get; set; }

        public long? LastDocsEtag { get; set; }
    }
}