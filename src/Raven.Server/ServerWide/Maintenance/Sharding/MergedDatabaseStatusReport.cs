using System.Collections.Generic;
using System.Linq;

namespace Raven.Server.ServerWide.Maintenance.Sharding
{
    public sealed class MergedDatabaseStatusReport
    {
        public Dictionary<ShardNumber, DatabaseStatusReport> MergedReport = new Dictionary<ShardNumber, DatabaseStatusReport>();

        public ShardReport[] GetShardsReports => MergedReport.Select(r => new ShardReport
        {
            Shard = r.Key,
            ReportPerBucket = r.Value.ReportPerBucket
        }).ToArray();
    }
}
