using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Imports.metrics.Core;

namespace Raven.Database.Extensions
{
    internal static class MetricExtensions
    {
        public static HistogramData CreateHistogramData(this HistogramMetric self)
        {
            double[] percentiles = self.Percentiles(0.5, 0.75, 0.95, 0.99, 0.999, 0.9999);

            return new HistogramData
            {
                Counter = self.Count,
                Max = self.Max,
                Mean = self.Mean,
                Min = self.Min,
                Stdev = self.StdDev,
                Percentiles = new Dictionary<string, double>
                {
                        {"50%", percentiles[0]},
                        {"75%", percentiles[1]},
                        {"95%", percentiles[2]},
                        {"99%", percentiles[3]},
                        {"99.9%", percentiles[4]},
                        {"99.99%", percentiles[5]},
                }
            };
        }

        public static MeterData CreateMeterData(this MeterMetric self)
        {
            return new MeterData
            {
                Count = self.Count,
                FifteenMinuteRate = Math.Round(self.FifteenMinuteRate, 3),
                FiveMinuteRate = Math.Round(self.FiveMinuteRate, 3),
                MeanRate = Math.Round(self.MeanRate, 3),
                OneMinuteRate = Math.Round(self.OneMinuteRate, 3),
            };
        }
        
        public static Dictionary<string, HistogramData> ToHistogramDataDictionary(this ConcurrentDictionary<string, HistogramMetric> self)
        {
            return self.ToDictionary(x => x.Key, x => CreateHistogramData(x.Value));
        }

        public static Dictionary<string, MeterData> ToMeterDataDictionary(this ConcurrentDictionary<string, MeterMetric> self)
        {
            return self.ToDictionary(x => x.Key, x => CreateMeterData(x.Value));
        }
    }
}
