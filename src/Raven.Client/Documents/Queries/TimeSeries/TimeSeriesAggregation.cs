using System;
using System.Collections.Generic;
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

    public class TimeSeriesRawResult<TValues> : TimeSeriesRawResult where TValues : new()
    {
        public new TimeSeriesEntry<TValues>[] Results { get; set; }
    }

    public class TimeSeriesAggregationResult : TimeSeriesQueryResult
    {
        public TimeSeriesRangeAggregation[] Results { get; set; }
    }

    public class TimeSeriesRangeAggregation
    {
        public long[] Count;
        public double[] Max, Min, Last, First, Average, Sum;
        public DateTime To, From;
    }

    public class TimeSeriesAggregationResult<T> : TimeSeriesAggregationResult where T : new()
    {
        public new TimeSeriesRangeAggregation<T>[] Results { get; set; }
    }

    public class TimeSeriesRangeAggregation<T> : TimeSeriesRangeAggregation where T : new()
    {
        private T _max;
        private T _min;
        private T _last;
        private T _first;
        private T _average;
        private T _sum;
        private T _count;

        [JsonIgnore]
        public new T Max
        {
            get
            {
                if (EqualityComparer<T>.Default.Equals(_max, default) == false)
                    return _max;

                _max = TimeSeriesValuesHelper.SetMembers<T>(
                    base.Max ?? throw new InvalidOperationException($"'{nameof(Max)}' is not found in the results. Maybe you forget to 'select max()' in the query?"));
                return _max;
            }
        }

        [JsonIgnore]
        public new T Min
        {
            get
            {
                if (EqualityComparer<T>.Default.Equals(_min, default) == false)
                    return _min;

                _min = TimeSeriesValuesHelper.SetMembers<T>(
                    base.Min ?? throw new InvalidOperationException($"'{nameof(Min)}' is not found in the results. Maybe you forget to 'select min()' in the query?"));
                return _min;
            }
        }

        [JsonIgnore]
        public new T Last
        {
            get
            {
                if (EqualityComparer<T>.Default.Equals(_last, default) == false)
                    return _last;

                _last = TimeSeriesValuesHelper.SetMembers<T>(
                    base.Last ?? throw new InvalidOperationException($"'{nameof(Last)}' is not found in the results. Maybe you forget to 'select last()' in the query?"));
                return _last;
            }
        }

        [JsonIgnore]
        public new T First
        {
            get
            {
                if (EqualityComparer<T>.Default.Equals(_first, default) == false)
                    return _first;

                _first = TimeSeriesValuesHelper.SetMembers<T>(
                    base.First ?? throw new InvalidOperationException($"'{nameof(First)}' is not found in the results. Maybe you forget to 'select first()' in the query?"));
                return _first;
            }
        }

        [JsonIgnore]
        public new T Average
        {
            get
            {
                if (EqualityComparer<T>.Default.Equals(_average, default) == false)
                    return _average;

                _average = TimeSeriesValuesHelper.SetMembers<T>(
                    base.Average ?? throw new InvalidOperationException($"'{nameof(Average)}' is not found in the results. Maybe you forget to 'select avg()' in the query?"));
                return _average;
            }
        }

        [JsonIgnore]
        public new T Sum
        {
            get
            {
                if (EqualityComparer<T>.Default.Equals(_sum, default) == false)
                    return _sum;

                _sum = TimeSeriesValuesHelper.SetMembers<T>(
                    base.Sum ?? throw new InvalidOperationException($"'{nameof(Sum)}' is not found in the results. Maybe you forget to 'select sum()' in the query?"));
                return _sum;
            }
        }

        [JsonIgnore]
        public new T Count
        {
            get
            {
                if (EqualityComparer<T>.Default.Equals(_count, default) == false)
                    return _count;

                _count = TimeSeriesValuesHelper.SetMembers<T>(Array.ConvertAll<long, double>(
                    base.Count ?? throw new InvalidOperationException($"'{nameof(Count)}' is not found in the results. Maybe you forget to 'select count()' in the query?"), x => x));
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
