using System.Collections.Concurrent;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Database.Util;
using Raven.Server.Utils;
using Raven.Server.Utils.Metrics;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.SqlReplication
{
    public class SqlReplicationMetricsCountersManager
    {
        public MeterMetric SqlReplicationBatchSizeMeter { get; private set; }
        public ConcurrentDictionary<string, SqlReplicationTableMetrics> TablesMetrics { get; set; }
        public ConcurrentQueue<SqlReplicationPerformanceStats> ReplicationPerformanceStats { get; set; }

        public SqlReplicationMetricsCountersManager()
        {
            SqlReplicationBatchSizeMeter = new MeterMetric();
            TablesMetrics = new ConcurrentDictionary<string, SqlReplicationTableMetrics>();
            ReplicationPerformanceStats = new ConcurrentQueue<SqlReplicationPerformanceStats>();
        }

        public SqlReplicationTableMetrics GetTableMetrics(string tableName)
        {
            return TablesMetrics.GetOrAdd(tableName, name => new SqlReplicationTableMetrics(name));
        }

        public DynamicJsonValue ToSqlReplicationMetricsData()
        {
            return new DynamicJsonValue
            {
                ["GeneralMetrics"] = new DynamicJsonValue
                {
                    ["Batch Size Meter"] = SqlReplicationBatchSizeMeter.CreateMeterData()
                },
                ["TablesMetrics"] = TablesMetrics.ToDictionary(x => x.Key, x => x.Value.ToSqlReplicationTableMetricsDataDictionary()),
            };
        }

        public void UpdateReplicationPerformance(SqlReplicationPerformanceStats performance)
        {
            ReplicationPerformanceStats.Enqueue(performance);
            while (ReplicationPerformanceStats.Count > 25)
            {
                SqlReplicationPerformanceStats _;
                ReplicationPerformanceStats.TryDequeue(out _);
            }
        }
    }
}