using System.Collections.Generic;

namespace Raven.Abstractions.Data
{
    public class DatabaseMetrics
    {
        public double DocsWritesPerSecond { get; set; }

        public double IndexedPerSecond { get; set; }

        public double ReducedPerSecond { get; set; }

        public double RequestsPerSecond { get; set; }

        public MeterData Requests { get; set; }

        public HistogramData RequestsDuration { get; set; }

        public class HistogramData
        {
            public HistogramData()
            {
                Percentiles = new Dictionary<string, double>();
            }

            public long Counter { get; set; }
            public double Max { get; set; }
            public double Min { get; set; }
            public double Mean { get; set; }
            public double Stdev { get; set; }
            public Dictionary<string, double> Percentiles { get; set; }
        }

        public class MeterData
        {
            public long Count { get; set; }
            public double MeanRate { get; set; }
            public double OneMinuteRate { get; set; }
            public double FiveMinuteRate { get; set; }
            public double FifteenMinuteRate { get; set; }
        }
    }
}