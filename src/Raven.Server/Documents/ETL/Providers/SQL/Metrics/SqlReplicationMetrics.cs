using Raven.Server.Utils.Metrics;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL.Providers.SQL.Metrics
{
    public class SqlEtlTableMetrics
    {
        public readonly string TableName;
        public readonly MeterMetric DeleteActionsMeter;
        public readonly MeterMetric InsertActionsMeter;

        public SqlEtlTableMetrics(string tableName)
        {
            TableName = tableName;
            DeleteActionsMeter = new MeterMetric();
            InsertActionsMeter = new MeterMetric();
        }

        public DynamicJsonValue ToSqlEtlTableMetricsDataDictionary()
        {
            return new DynamicJsonValue
            {
                ["Delete Actions Meter"] = DeleteActionsMeter.CreateMeterData(),
                ["Insert Actions Meter"] = InsertActionsMeter.CreateMeterData()
            };
        }
    }
}
