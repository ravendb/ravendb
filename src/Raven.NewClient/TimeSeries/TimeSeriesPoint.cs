using System;

namespace Raven.NewClient.Abstractions.TimeSeries
{
    public class TimeSeriesPointId
    {
        public string Type { get; set; }
        public string Key { get; set; }
        public DateTimeOffset At { get; set; }
    }

    public class TimeSeriesPoint
    {
#if DEBUG
        public string DebugKey { get; set; }
#endif

        public DateTimeOffset At { get; set; }

        public double[] Values { get; set; }
    }

    public class TimeSeriesFullPoint
    {
        public string Type { get; set; }

        public string Key { get; set; }

        public DateTimeOffset At { get; set; }

        public double[] Values { get; set; }
    }
}
