using Metrics.Sampling;

namespace Metrics.MetricData
{
    /// <summary>
    /// The value reported by a Histogram Metric
    /// </summary>
    public class HistogramValue:IMetricValue
    {
        public readonly long Count;

        public readonly double LastValue;
        public readonly double Max;
        public readonly double Mean;
        public readonly double Min;
        public readonly double StdDev;
        public readonly double Median;
        public readonly double Percentile75;
        public readonly double Percentile95;
        public readonly double Percentile98;
        public readonly double Percentile99;
        public readonly double Percentile999;
        public readonly int SampleSize;
        public string Name { get; private set; }

        public HistogramValue(string name,double lastValue, Snapshot snapshot)
            : this(name,snapshot.Count,
            lastValue,
            snapshot.Max,
            snapshot.Mean,
            snapshot.Min,
            snapshot.StdDev,
            snapshot.Median,
            snapshot.Percentile75,
            snapshot.Percentile95,
            snapshot.Percentile98,
            snapshot.Percentile99,
            snapshot.Percentile999,
            snapshot.Size)
        { }

        public HistogramValue(string name, long count,
            double lastValue,
            double max,
            double mean,
            double min,
            double stdDev,
            double median,
            double percentile75,
            double percentile95,
            double percentile98,
            double percentile99,
            double percentile999,
            int sampleSize)
        {
            this.Count = count;
            this.LastValue = lastValue;
            this.Max = max;
            this.Mean = mean;
            this.Min = min;
            this.StdDev = stdDev;
            this.Median = median;
            this.Percentile75 = percentile75;
            this.Percentile95 = percentile95;
            this.Percentile98 = percentile98;
            this.Percentile99 = percentile99;
            this.Percentile999 = percentile999;
            this.SampleSize = sampleSize;
            this.Name = name;
        }

        public void Dispose()
        {
            
        }
    }
}
