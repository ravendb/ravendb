using Raven.Server.Utils.Metrics;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL.Providers.RelationalDatabase.Common.Metrics;

 public sealed class RelationalDatabaseEtlTableMetrics(string tableName)
 {
        public readonly string TableName = tableName;
        public readonly MeterMetric DeleteActionsMeter = new();
        public readonly MeterMetric InsertActionsMeter = new();

        public DynamicJsonValue ToRelationalEtlTableMetricsDataDictionary()
        {
            return new DynamicJsonValue
            {
                ["Delete Actions Meter"] = DeleteActionsMeter.CreateMeterData(),
                ["Insert Actions Meter"] = InsertActionsMeter.CreateMeterData()
            };
        }
    }

