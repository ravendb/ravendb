using System;
using Raven.Server.Utils;
using Sparrow.Server.Utils;

namespace Raven.Server.Documents
{
    public class DatabaseMetricCacher : MetricCacher
    {
        private readonly DocumentDatabase _database;

        public DatabaseMetricCacher(DocumentDatabase database)
        {
            _database = database;
        }

        public void Initialize()
        {
            Register(MetricCacher.Keys.Database.DiskSpaceInfo, TimeSpan.FromSeconds(30), CalculateDiskSpaceInfo);
        }

        private DiskSpaceResult CalculateDiskSpaceInfo()
        {
            return DiskUtils.GetDiskSpaceInfo(_database.Configuration.Core.DataDirectory.FullPath);
        }
    }
}
