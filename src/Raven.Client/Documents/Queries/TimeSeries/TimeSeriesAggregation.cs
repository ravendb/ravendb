using System;
using Newtonsoft.Json;
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

    public class TimeSeriesRawResult<TValues> : TimeSeriesRawResult where TValues : TimeSeriesEntry
    {
        public new TValues[] Results { get; set; }
    }

    public class TimeSeriesAggregationResult : TimeSeriesQueryResult
    {
        public TimeSeriesRangeAggregation[] Results { get; set; }
    }

    public class TimeSeriesRangeAggregation
    {
        public long[] Count;
        public double[] Max, Min, Last, First, Average;
        public DateTime To, From;
    }

    public class TimeSeriesAggregationResult<T> : TimeSeriesAggregationResult where T : TimeSeriesAggregatedEntry, new()
    {
        public new TimeSeriesRangeAggregation<T>[] Results { get; set; }
    }

    public class TimeSeriesRangeAggregation<T> : TimeSeriesRangeAggregation where T : TimeSeriesAggregatedEntry, new()
    {
        private T _max;
        private T _min;
        private T _last;
        private T _first;
        private T _average;
        private T _count;

        [JsonIgnore]
        public new T Max
        {
            get
            {
                _max ??= new T();

                if (_max.Values != null)
                    return _max;

                _max.Values = base.Max;
                _max.SetMembersFromValues();
                return _max;
            }
        }

        [JsonIgnore]
        public new T Min
        {
            get
            {
                _min ??= new T();

                if (_min.Values != null)
                    return _min;

                _min.Values = base.Min;
                _min.SetMembersFromValues();
                return _min;
            }
        }

        [JsonIgnore]
        public new T Last
        {
            get
            {
                _last ??= new T();

                if (_last.Values != null)
                    return _last;

                _last.Values = base.Last;
                _last.SetMembersFromValues();
                return _last;
            }
        }

        [JsonIgnore]
        public new T First 
        {
            get
            {
                _first ??= new T();

                if (_first.Values != null)
                    return _first;

                _first.Values = base.First;
                _first.SetMembersFromValues();
                return _first;
            }
        }

        [JsonIgnore]
        public new T Average 
        {
            get
            {
                _average ??= new T();

                if (_average.Values != null)
                    return _average;

                _average.Values = base.Average;
                _average.SetMembersFromValues();
                return _average;
            }
        }

        [JsonIgnore]
        public new T Count
        {
            get
            {
                _count ??= new T();

                if (_count.Values != null)
                    return _count;

                _count.Values = Array.ConvertAll<long, double>(base.Count, x => x);
                _count.SetMembersFromValues();
                return _count;
            }
        }
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
