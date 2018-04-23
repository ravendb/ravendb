using System;
using System.Collections.Generic;

namespace Raven.Server.Documents.PeriodicBackup.Restore
{
    public class RestorePoints
    {
        public RestorePoints()
        {
            List = new List<RestorePoint>();
        }

        public List<RestorePoint> List { get; set; }
    }

    public class RestorePoint
    {
        public DateTime DateTime { get; set; }

        public string Location { get; set; }

        public string FileName { get; set; }

        public bool IsSnapshotRestore { get; set; }

        public bool IsIncremental { get; set; }

        public int FilesToRestore { get; set; }

        public string DatabaseName { get; set; }

        public string NodeTag { get; set; }
    }
}
