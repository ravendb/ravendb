using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using metrics.Core;
using Raven.Abstractions.Data;

namespace Raven.Database.Extensions
{
    public static class MetricExtensions
    {
        public static MeterData GetMeterData(this MeterMetric metric,int accuracy)
        {
            return new MeterData()
            {
                Count = metric.Count,
                FifteenMinuteRate = Math.Round(metric.FifteenMinuteRate, accuracy),
                FiveMinuteRate = Math.Round(metric.FiveMinuteRate, accuracy),
                MeanRate = Math.Round(metric.MeanRate, accuracy),
                OneMinuteRate = Math.Round(metric.OneMinuteRate, accuracy)
            };
        }

        public static HistogramData GetHistogramData(this HistogramMetric histogram, double[] percentiles)
        {
            var percentileValues = histogram.Percentiles(percentiles);

            return new HistogramData
            {
                Counter = histogram.Count,
                Max = histogram.Max,
                Mean = histogram.Mean,
                Min = histogram.Min,
                Stdev = histogram.StdDev,
                Percentiles = new Dictionary<string, double>
                {
                    {string.Format("{0}%", percentiles[0]), percentileValues[0]},
                    {string.Format("{0}%", percentiles[1]), percentileValues[1]},
                    {string.Format("{0}%", percentiles[2]), percentileValues[2]},
                    {string.Format("{0}%", percentiles[3]), percentileValues[3]},
                    {string.Format("{0}%", percentiles[4]), percentileValues[4]},
                    {string.Format("{0}%", percentiles[5]), percentileValues[5]},
                }
            };
        }
    }
}
