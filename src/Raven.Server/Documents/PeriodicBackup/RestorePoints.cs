using System.Collections.Generic;

namespace Raven.Server.Documents.PeriodicBackup
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
        public string Key { get; set; }
        
        public RestorePointDetails Details { get; set; }
    }

    public class RestorePointDetails
    {
        public string Location { get; set; }

        public string FileName { get; set; }

        public bool IsSnapshotRestore { get; set; }

        public int FilesToRestore { get; set; }
    }
}