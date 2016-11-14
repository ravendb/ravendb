using System;

namespace Raven.NewClient.Abstractions.TimeSeries
{
    public class TimeSeriesKeySummary
    {
        public TimeSeriesType Type { get; set; }
        public string Key { get; set; }
        public long PointsCount { get; set; }
        public DateTimeOffset MinPoint { get; set; }
        public DateTimeOffset MaxPoint { get; set; }
    }

    public class TimeSeriesKey
    {
        public TimeSeriesType Type { get; set; }
        public string Key { get; set; }
        public long PointsCount { get; set; }
    }
}
