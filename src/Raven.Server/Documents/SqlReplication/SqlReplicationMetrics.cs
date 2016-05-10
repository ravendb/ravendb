using Raven.Database.Util;
using Raven.Server.Utils;
using Raven.Server.Utils.Metrics;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.SqlReplication
{
    public class SqlReplicationTableMetrics
    {
        public readonly string TableName;
        public readonly MeterMetric SqlReplicationDeleteActionsMeter;
        public readonly MeterMetric SqlReplicationInsertActionsMeter;

        public SqlReplicationTableMetrics(MetricsScheduler metricsScheduler, string tableName)
        {
            TableName = tableName;
            SqlReplicationDeleteActionsMeter = new MeterMetric(metricsScheduler);
            SqlReplicationInsertActionsMeter = new MeterMetric(metricsScheduler);
        }

        public DynamicJsonValue ToSqlReplicationTableMetricsDataDictionary()
        {
            return new DynamicJsonValue
            {
                ["Delete Actions Meter"] = SqlReplicationDeleteActionsMeter.CreateMeterData(),
                ["Insert Actions Meter"] = SqlReplicationInsertActionsMeter.CreateMeterData(),
            };
        }
    }
}
