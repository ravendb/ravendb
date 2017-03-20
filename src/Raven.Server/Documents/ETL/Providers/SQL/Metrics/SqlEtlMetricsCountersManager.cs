using System.Collections.Concurrent;
using System.Linq;
using Raven.Server.Utils.Metrics;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL.Providers.SQL.Metrics
{
    public class SqlEtlMetricsCountersManager
    {
        public MeterMetric BatchSizeMeter { get; private set; }
        public ConcurrentDictionary<string, SqlEtlTableMetrics> TablesMetrics { get; set; }
        public ConcurrentQueue<SqlEtlPerformanceStats> PerformanceStats { get; set; }

        public SqlEtlMetricsCountersManager()
        {
            BatchSizeMeter = new MeterMetric();
            TablesMetrics = new ConcurrentDictionary<string, SqlEtlTableMetrics>();
            PerformanceStats = new ConcurrentQueue<SqlEtlPerformanceStats>();
        }

        public SqlEtlTableMetrics GetTableMetrics(string tableName)
        {
            return TablesMetrics.GetOrAdd(tableName, name => new SqlEtlTableMetrics(name));
        }

        public DynamicJsonValue ToSqlEtlMetricsData()
        {
            return new DynamicJsonValue
            {
                ["GeneralMetrics"] = new DynamicJsonValue
                {
                    ["Batch Size Meter"] = BatchSizeMeter.CreateMeterData()
                },
                ["TablesMetrics"] = TablesMetrics.ToDictionary(x => x.Key, x => x.Value.ToSqlEtlTableMetricsDataDictionary()),
            };
        }

        public void UpdateReplicationPerformance(SqlEtlPerformanceStats performance)
        {
            PerformanceStats.Enqueue(performance);
            while (PerformanceStats.Count > 25)
            {
                SqlEtlPerformanceStats _;
                PerformanceStats.TryDequeue(out _);
            }
        }
    }
}