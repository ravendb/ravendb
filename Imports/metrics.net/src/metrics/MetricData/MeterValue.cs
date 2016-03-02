using System;
using System.Linq;
using Metrics.Utils;

namespace Metrics.MetricData
{
    /// <summary>
    /// The value reported by a Meter Metric
    /// </summary>
    public class MeterValue:IMetricValue
    {
        public readonly long Count;
        public readonly double MeanRate;
        public readonly double OneMinuteRate;
        public readonly double FiveMinuteRate;
        public readonly double FifteenMinuteRate;
        public string Name { get; private set; }
        

        public MeterValue(string name, long count, double meanRate, double oneMinuteRate, double fiveMinuteRate, double fifteenMinuteRate)
        {
            this.Count = count;
            this.MeanRate = meanRate;
            this.OneMinuteRate = oneMinuteRate;
            this.FiveMinuteRate = fiveMinuteRate;
            this.FifteenMinuteRate = fifteenMinuteRate;
            this.Name = name;
        }


        public void Dispose()
        {
            
        }
    }
}
