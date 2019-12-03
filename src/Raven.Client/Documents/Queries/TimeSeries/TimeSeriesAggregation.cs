using System;

namespace Raven.Client.Documents.Queries.TimeSeries
{
    public class TimeSeriesRangeAggregation
    {
        public long[] Count;
        public double?[] Max, Min, Last, First, Avg;
        public DateTime To, From;
    }

    public class TimeSeriesAggregation
    {
        public long Count { get; set; }
        public TimeSeriesRangeAggregation[] Results { get; set; }
    }
}
