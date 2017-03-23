using Raven.Server.Utils.Metrics;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL.Metrics
{
    public class EtlMetricsCountersManager
    {
        public EtlMetricsCountersManager()
        {
            BatchSizeMeter = new MeterMetric();
        }

        public MeterMetric BatchSizeMeter { get; protected set; }

        public virtual DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(BatchSizeMeter)] = BatchSizeMeter.CreateMeterData()
            };
        }
    }
}