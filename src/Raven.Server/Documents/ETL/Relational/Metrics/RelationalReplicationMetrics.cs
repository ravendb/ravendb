using Raven.Server.Utils.Metrics;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL.Relational.Metrics;

 public sealed class RelationalEtlTableMetrics
    {
        public readonly string TableName;
        public readonly MeterMetric DeleteActionsMeter;
        public readonly MeterMetric InsertActionsMeter;

        public RelationalEtlTableMetrics(string tableName)
        {
            TableName = tableName;
            DeleteActionsMeter = new MeterMetric();
            InsertActionsMeter = new MeterMetric();
        }

        public DynamicJsonValue ToRelationalEtlTableMetricsDataDictionary()
        {
            return new DynamicJsonValue
            {
                ["Delete Actions Meter"] = DeleteActionsMeter.CreateMeterData(),
                ["Insert Actions Meter"] = InsertActionsMeter.CreateMeterData()
            };
        }
    }

