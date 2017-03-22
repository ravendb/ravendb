using System.Collections.Concurrent;
using Raven.Server.Utils.Metrics;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL.Metrics
{
    public class EtlMetricsCountersManager
    {
        public EtlMetricsCountersManager()
        {
            BatchSizeMeter = new MeterMetric();
            PerformanceStats = new ConcurrentQueue<EtlPerformanceStats>();
        }

        public MeterMetric BatchSizeMeter { get; protected set; }

        public ConcurrentQueue<EtlPerformanceStats> PerformanceStats { get; set; }

        public void UpdatePerformanceStats(EtlPerformanceStats performance)
        {
            PerformanceStats.Enqueue(performance);
            while (PerformanceStats.Count > 25)
            {
                EtlPerformanceStats _;
                PerformanceStats.TryDequeue(out _);
            }
        }

        public virtual DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(BatchSizeMeter)] = BatchSizeMeter.CreateMeterData()
            };
        }
    }
}