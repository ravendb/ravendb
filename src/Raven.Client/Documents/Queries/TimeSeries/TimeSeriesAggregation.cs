using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Raven.Client.Documents.Session.Operations;
using Raven.Client.Documents.Session.TimeSeries;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Queries.TimeSeries
{
    public class TimeSeriesQueryResult
    {
        public long Count { get; set; }
    }

    public interface ITimeSeriesQueryStreamEntry
    {

    }

    internal interface ITimeSeriesQueryStreamResult
    {
        void SetStream(StreamOperation.TimeSeriesStreamEnumerator stream);
    }

    internal class TimeSeriesStreamEnumerator<T> : IAsyncEnumerator<T>, IEnumerator<T> where T : ITimeSeriesQueryStreamEntry 
    {
        private readonly IAsyncEnumerator<BlittableJsonReaderObject> _outer;
        private readonly CancellationToken _token;

        private static readonly Func<BlittableJsonReaderObject, T> Converter = JsonDeserializationBase.GenerateJsonDeserializationRoutine<T>();

        internal TimeSeriesStreamEnumerator(StreamOperation.TimeSeriesStreamEnumerator outer)
        {
            _outer = outer;
        }

        internal TimeSeriesStreamEnumerator(IAsyncEnumerator<BlittableJsonReaderObject> outer, CancellationToken token)
        {
            _outer = outer;
            _token = token;
        }

        public bool MoveNext()
        {
            return AsyncHelpers.RunSync(MoveNextAsync().AsTask);
        }

        public void Reset()
        {
            throw new NotSupportedException("Enumerator does not support resetting");
        }

        public async ValueTask<bool> MoveNextAsync()
        {
            if (await _outer.MoveNextAsync().ConfigureAwait(false) == false)
            {
                Current = default;
                return false;
            }
            
            _token.ThrowIfCancellationRequested();
            using (_outer.Current)
            {
                Current = Converter(_outer.Current);
            }
            return true;
        }

        public T Current { get; private set; } 

        object IEnumerator.Current => Current;

        public void Dispose()
        {
            AsyncHelpers.RunSync(DisposeAsync().AsTask);
        }

        public ValueTask DisposeAsync()
        {
            return _outer.DisposeAsync();
        }
    }

    public abstract class TimeSeriesQueryStreamResultBase<TResult> : TimeSeriesQueryResult, ITimeSeriesQueryStreamResult where TResult : ITimeSeriesQueryStreamEntry
    {
        public TResult[] Results
        {
            get => _results ?? MaterializeStream();
            set => _results = value;
        }

        [JsonIgnore]
        private TResult[] _results;

        private TResult[] MaterializeStream()
        {
            if (Stream == null)
                return null;

            var list = new List<TResult>();
            while (Stream.MoveNext())
            {
                list.Add(Stream.Current);
            }
            _results = list.ToArray();
            return _results;
        }

        [JsonIgnore]
        public IEnumerator<TResult> Stream => _timeSeriesStream ?? _results?.AsEnumerable().GetEnumerator();

        [JsonIgnore]
        public IAsyncEnumerator<TResult> StreamAsync => _timeSeriesStream;

        [JsonIgnore]
        private TimeSeriesStreamEnumerator<TResult> _timeSeriesStream;
        
        void ITimeSeriesQueryStreamResult.SetStream(StreamOperation.TimeSeriesStreamEnumerator stream)
        {
            _timeSeriesStream = new TimeSeriesStreamEnumerator<TResult>(stream);
        }
    }
    public class TimeSeriesRawResult : TimeSeriesQueryStreamResultBase<TimeSeriesEntry>
    {
    }

    public class TimeSeriesRawResult<T> : TimeSeriesQueryStreamResultBase<TimeSeriesEntry<T>> where T : new()
    {
    }

    public class TimeSeriesAggregationResult : TimeSeriesQueryStreamResultBase<TimeSeriesRangeAggregation>
    {
    }

    public class TimeSeriesAggregationResult<T> : TimeSeriesQueryStreamResultBase<TimeSeriesRangeAggregation<T>> where T : new()
    {
    }

    public class TimeSeriesRangeAggregation : IPostJsonDeserialization, ITimeSeriesQueryStreamEntry
    {
        public long[] Count;
        public double[] Max, Min, Last, First, Average, Sum;
        public DateTime To, From;

        public string Key { get; private set; }

        [OnDeserialized]
        internal void OnNewtonSoftJsonDeserialized(StreamingContext context)
        {
            SetMinMaxDateTime();
        }

        void IPostJsonDeserialization.PostDeserialization()
        {
            SetMinMaxDateTime();
        }
        
        private void SetMinMaxDateTime()
        {
            if (From == default)
                From = DateTime.MinValue;
            if (To == default)
                To = DateTime.MaxValue;
        }
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
