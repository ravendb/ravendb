using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using Raven.Client.Documents.Queries.TimeSeries;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Queries.Results
{
    public class AggregationHolder
    {
        public static readonly object NullBucket = new object();

        // local pool, for current query
        private readonly ObjectPool<TimeSeriesAggregation[], TimeSeriesAggregationReset> _pool;
        private readonly int _poolSize = 32;

        private readonly DocumentsOperationContext _context;
        private readonly InterpolationType _interpolationType;

        private readonly AggregationType[] _types;
        private readonly string[] _names;

        private Dictionary<object, TimeSeriesAggregation[]> _current;
        private Dictionary<object, PreviousAggregation> _previous;

        public bool HasValues => _current?.Count > 0;

        public AggregationHolder(DocumentsOperationContext context, Dictionary<AggregationType, string> types, InterpolationType interpolationType)
        {
            _context = context;

            _names = types.Values.ToArray();
            _types = types.Keys.ToArray();

            _interpolationType = interpolationType;
            if (_interpolationType != InterpolationType.None)
                _poolSize *= 2;

            _pool = new ObjectPool<TimeSeriesAggregation[], TimeSeriesAggregationReset>(TimeSeriesAggregationFactory, _poolSize);
        }

        private TimeSeriesAggregation[] TimeSeriesAggregationFactory()
        {
            var bucket = new TimeSeriesAggregation[_types.Length];
            for (int i = 0; i < _types.Length; i++)
            {
                var type = _types[i];
                var name = _names?[i];
                bucket[i] = new TimeSeriesAggregation(type, name);
            }

            return bucket;
        }

        public TimeSeriesAggregation[] this[object bucket]
        {
            get
            {
                var key = Clone(bucket);

                _current ??= new Dictionary<object, TimeSeriesAggregation[]>();
                if (_current.TryGetValue(key, out var value))
                    return value;

                return _current[key] = _pool.Allocate();
            }
        }

        public IEnumerable<DynamicJsonValue> AddCurrentToResults(RangeGroup range, double? scale)
        {
            if (_interpolationType != InterpolationType.None)
            {
                foreach (var gap in FillMissingGaps(range.Start, scale))
                {
                    yield return gap;
                }
            }

            foreach (var kvp in _current)
            {
                var key = kvp.Key;
                var value = kvp.Value;

                if (value[0].Any == false)
                    continue;

                yield return ToJson(scale, range.Start, range.End, key, value);

                if (_interpolationType == InterpolationType.None)
                {
                    _pool.Free(value);
                    continue;
                }

                UpdatePrevious(key, range, value);
            }

            _current = null;
        }

        private DynamicJsonValue ToJson(double? scale, DateTime? from, DateTime? to, object key, TimeSeriesAggregation[] value)
        {
            if (from == DateTime.MinValue)
                from = null;
            if (to == DateTime.MaxValue)
                to = null;

            var result = new DynamicJsonValue
            {
                [nameof(TimeSeriesRangeAggregation.From)] = from, 
                [nameof(TimeSeriesRangeAggregation.To)] = to, 
                [nameof(TimeSeriesRangeAggregation.Key)] = GetNameFromKey(key)
            };

            for (int i = 0; i < value.Length; i++)
            {
                result[value[i].Name] = new DynamicJsonArray(value[i].GetFinalValues(scale).Cast<object>());
            }

            return result;
        }

        private IEnumerable<(object Key, PreviousAggregation Previous, TimeSeriesAggregation[] Current)> GetGapsPerBucket(DateTime to)
        {
            if (_current == null || _previous == null)
                yield break;

            foreach (var previous in _previous)
            {
                var key = previous.Key;
                if (_current.ContainsKey(key) == false)
                    continue;

                var gapData = previous.Value;
                if (gapData.Range.WithinNextRange(to))
                    continue;

                yield return (key, previous.Value, _current[key]);

                _previous.Remove(key);
                _pool.Free(previous.Value.Data);
            }
        }

        private object GetNameFromKey(object key)
        {
            if (key == NullBucket)
                return null;

            if (key is Document doc)
                return doc.Id;

            return key;
        }

        private void UpdatePrevious(object key, RangeGroup range, TimeSeriesAggregation[] values)
        {
            _previous ??= new Dictionary<object, PreviousAggregation>();
            if (_previous.TryGetValue(key, out var result) == false)
            {
                result = _previous[key] = new PreviousAggregation();
            }
            else
            {
                _pool.Free(result.Data);
            }

            result.Data = values;
            result.Range = range;
        }

        private object Clone(object value)
        {
            if (value == null || value == NullBucket)
                return NullBucket;

            if (value is LazyStringValue lsv)
                return lsv.ToString();

            if (value is string s)
                return s;

            if (value is ValueType)
                return value;

            if (value is LazyCompressedStringValue lcsv)
                return lcsv.ToString();

            if (value is LazyNumberValue lnv)
                return lnv.ToDouble(CultureInfo.InvariantCulture);

            if (value is DateTime time)
                return time.ToString("O");

            if (value is DateTimeOffset offset)
                return offset.ToString("O");

            if (value is TimeSpan span)
                return span.ToString("c");

            if (value is BlittableJsonReaderObject json)
            {
                return json.CloneOnTheSameContext();
            }

            if (value is BlittableJsonReaderArray arr)
            {
                return arr.Clone(_context);
            }

            if (value is Document doc)
                return doc;

            throw new NotSupportedException($"Unable to group by type: {value.GetType()}");
        }

        public class PreviousAggregation
        {
            public RangeGroup Range;

            public TimeSeriesAggregation[] Data;
        }

        private IEnumerable<DynamicJsonValue> FillMissingGaps(DateTime to, double? scale)
        {
            foreach (var result in GetGapsPerBucket(to))
            {
                var gapData = result.Previous;

                var from = gapData.Range.Start; // we have this point
                gapData.Range.MoveToNextRange();

                var start = gapData.Range.Start; // this one we miss
                var end = gapData.Range.End;

                var startData = result.Previous.Data;
                var endData = result.Current;

                Debug.Assert(start < to, "Invalid gap data");

                var point = start;

                switch (_interpolationType)
                {
                    case InterpolationType.Linear:
                        
                        Debug.Assert(startData.Length == endData.Length, "Invalid aggregation stats");

                        var numberOfAggregations = startData.Length;
                        var initial = new List<double>[numberOfAggregations];
                        var final = new List<double>[numberOfAggregations];
                        for (int i = 0; i < numberOfAggregations; i++)
                        {
                            Debug.Assert(startData[i].Aggregation == endData[i].Aggregation, "Invalid aggregation type");
                            initial[i] = new List<double>(startData[i].GetFinalValues(scale));
                            final[i] = new List<double>(endData[i].GetFinalValues(scale));
                        }

                        var numberOfValues = Math.Min(startData[0].NumberOfValues, endData[0].NumberOfValues);
                        var interpolated = new double[numberOfValues];

                        while (start < to)
                        {
                            var gap = new DynamicJsonValue
                            {
                                [nameof(TimeSeriesRangeAggregation.From)] = start,
                                [nameof(TimeSeriesRangeAggregation.To)] = end,
                                [nameof(TimeSeriesRangeAggregation.Key)] = GetNameFromKey(result.Key)
                            };

                            var quotient = (double)(point.Ticks - from.Ticks) / (to.Ticks - from.Ticks);
                            for (int i = 0; i < startData.Length; i++)
                            {
                                LinearInterpolation(quotient, initial[i], final[i], interpolated);
                                gap[startData[i].Name] = new DynamicJsonArray(interpolated.Cast<object>());
                            }
                            
                            yield return gap;

                            gapData.Range.MoveToNextRange();
                            start = gapData.Range.Start;
                            end = gapData.Range.End;

                            point = start;
                        }

                        break;
                    case InterpolationType.Nearest:

                        while (start < to)
                        {
                            var nearest = point - from <= to - point
                                ? startData
                                : endData;

                            yield return ToJson(scale, start, end, result.Key, nearest);

                            gapData.Range.MoveToNextRange();
                            start = gapData.Range.Start;
                            end = gapData.Range.End;

                            point = start;
                        }

                        break;
                    default:
                        throw new ArgumentOutOfRangeException("Unknown InterpolationType : " + _interpolationType);
                }
            }
        }

        private static void LinearInterpolation(double quotient, List<double> valuesA, List<double> valuesB, double[] result)
        {
            var minLength = Math.Min(valuesA.Count, valuesB.Count);
            if (minLength < valuesA.Count)
            {
                valuesA.RemoveRange(minLength - 1, valuesA.Count - minLength);
            }

            for (var index = 0; index < minLength; index++)
            {
                var yb = valuesB[index];
                var ya = valuesA[index];

                // y = yA + (yB - yA) * ((x - xa) / (xb - xa))
                result[index] = ya + (yb - ya) * quotient;
            }
        }

        private struct TimeSeriesAggregationReset : IResetSupport<TimeSeriesAggregation[]>
        {
            public void Reset(TimeSeriesAggregation[] values)
            {
                for (var index = 0; index < values.Length; index++)
                {
                    values[index].Init();
                }
            }
        }
    }
}
