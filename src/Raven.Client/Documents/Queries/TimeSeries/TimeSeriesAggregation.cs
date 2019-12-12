using System;
using Raven.Client.Documents.Session;

namespace Raven.Client.Documents.Queries.TimeSeries
{
    public class TimeSeriesQueryResult
    {
        public long Count { get; set; }
    }

    public class TimeSeriesRaw : TimeSeriesQueryResult
    {
        public TimeSeriesValue[] Results { get; set; }
    }

    public class TimeSeriesAggregation : TimeSeriesQueryResult
    {
        public TimeSeriesRangeAggregation[] Results { get; set; }
    }

    public class TimeSeriesRangeAggregation
    {
        public long[] Count;
        public double?[] Max, Min, Last, First, Avg;
        public DateTime To, From;
    }

}
