using System;
using Raven.Client.Documents.Session.TimeSeries;

namespace Raven.Client.Documents.Queries.TimeSeries
{
    public class TimeSeriesQueryResult
    {
        public long Count { get; set; }
    }

    public class TimeSeriesRawResult : TimeSeriesQueryResult
    {
        public TimeSeriesEntry[] Results { get; set; }
    }

    public class TimeSeriesAggregationResult : TimeSeriesQueryResult
    {
        public TimeSeriesRangeAggregation[] Results { get; set; }
    }

    public class TimeSeriesRangeAggregation
    {
        public long[] Count;
        public double?[] Max, Min, Last, First, Avg;
        public DateTime To, From;
    }

    public enum AggregationType
    {
        Min,
        Max,
        Mean,
        Avg,
        First,
        Last,
        Sum,
        Count,
    }
}
