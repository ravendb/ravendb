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
        public double?[] Max, Min, Last, First, Average;
        public DateTime To, From;
    }

    public enum AggregationType
    {
        // The order here matters.
        // When executing an aggregation query over rolled-up series,
        // we take just the appropriate aggregated value from each entry, 
        // according to the aggregation's position in this enum (e.g. AggregationType.Min => take entry.Values[2])

        First = 0,
        Last = 1,
        Min = 2,
        Max = 3,
        Sum = 4,
        Count = 5,
        Average = 6
    }
}
