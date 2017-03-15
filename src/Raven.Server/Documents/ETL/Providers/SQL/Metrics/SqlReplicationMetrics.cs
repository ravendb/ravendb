using Raven.Server.Utils.Metrics;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL.Providers.SQL.Metrics
{
    public class SqlReplicationTableMetrics
    {
        public readonly string TableName;
        public readonly MeterMetric SqlReplicationDeleteActionsMeter;
        public readonly MeterMetric SqlReplicationInsertActionsMeter;

        public SqlReplicationTableMetrics(string tableName)
        {
            TableName = tableName;
            SqlReplicationDeleteActionsMeter = new MeterMetric();
            SqlReplicationInsertActionsMeter = new MeterMetric();
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
