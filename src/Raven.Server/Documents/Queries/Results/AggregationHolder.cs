using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Raven.Client.Documents.Queries.TimeSeries;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Queries.AST;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server;
using Voron;

namespace Raven.Server.Documents.Queries.Results
{
    public class AggregationHolder
    {
        // local pool, for current query
        private readonly ObjectPool<TimeSeriesAggregation[], TimeSeriesAggregationReset> _pool;

        private readonly ByteStringContext _context;
        private readonly InterpolationType _interpolationType;

        private readonly AggregationType[] _types;
        private readonly string[] _names;

        private Dictionary<ulong, TimeSeriesAggregation[]> _current;
        private Dictionary<ulong, PreviousAggregation> _previous;

        private Dictionary<ulong, string> _keyNames;
        private Dictionary<object, ulong> _keyCache;

        public bool HasValues => _current?.Count > 0;

        public AggregationHolder(ByteStringContext context, Dictionary<AggregationType, string> types, InterpolationType interpolationType)
        {
            _context = context;

            _names = types.Values.ToArray();
            _types = types.Keys.ToArray();

            _interpolationType = interpolationType;
            _pool = new ObjectPool<TimeSeriesAggregation[], TimeSeriesAggregationReset>(TimeSeriesAggregationFactory);
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
                var key = GetKey(bucket);
                _current ??= new Dictionary<ulong, TimeSeriesAggregation[]>();
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

        private DynamicJsonValue ToJson(double? scale, DateTime? from, DateTime? to, ulong key, TimeSeriesAggregation[] value)
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

        private IEnumerable<(ulong Key, PreviousAggregation Previous, TimeSeriesAggregation[] Current)> GetGapsPerBucket(DateTime to)
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

        private string GetNameFromKey(ulong key)
        {
            if (_keyNames == null)
                return null;

            if (_keyNames.TryGetValue(key, out var name))
                return name;

            return null;
        }

        private void UpdatePrevious(ulong key, RangeGroup range, TimeSeriesAggregation[] values)
        {
            _previous ??= new Dictionary<ulong, PreviousAggregation>();
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

        private ulong GetKey(object value)
        {
            if (value == null)
                return 0;

            _keyCache ??= new Dictionary<object, ulong>();
            if (_keyCache.TryGetValue(value, out var key) == false)
                key = _keyCache[key] = CalculateKey(value);

            _keyNames ??= new Dictionary<ulong, string>();
            _keyNames.TryAdd(key, value.ToString());
            return key;
        }

        private unsafe ulong CalculateKey(object value)
        {
            if (value == null || value is DynamicNullObject)
                return 0;

            if (value is LazyStringValue lsv)
                return Hashing.XXHash64.Calculate(lsv.Buffer, (ulong)lsv.Size);

            if (value is string s)
            {
                using (Slice.From(_context, s, out Slice str))
                    return Hashing.XXHash64.Calculate(str.Content.Ptr, (ulong)str.Size);
            }

            if (value is LazyCompressedStringValue lcsv)
                return Hashing.XXHash64.Calculate(lcsv.Buffer, (ulong)lcsv.CompressedSize);

            if (value is long l)
            {
                unchecked
                {
                    return (ulong)l;
                }
            }

            if (value is ulong ul)
                return ul;

            if (value is decimal d)
                return Hashing.XXHash64.Calculate((byte*)&d, sizeof(decimal));

            if (value is int num)
                return (ulong)num;

            if (value is bool b)
                return b ? 1UL : 2UL;

            if (value is double dbl)
                return (ulong)dbl;

            if (value is LazyNumberValue lnv)
                return CalculateKey(lnv.Inner);

            long? ticks = null;
            if (value is DateTime time)
                ticks = time.Ticks;
            if (value is DateTimeOffset offset)
                ticks = offset.Ticks;
            if (value is TimeSpan span)
                ticks = span.Ticks;

            if (ticks.HasValue)
            {
                var t = ticks.Value;
                return (ulong)t;
            }

            if (value is BlittableJsonReaderObject json)
            {
                var hash = 0UL;
                var prop = new BlittableJsonReaderObject.PropertyDetails();

                for (int i = 0; i < json.Count; i++)
                {
                    // this call ensures properties to be returned in the same order, regardless their storing order
                    json.GetPropertyByIndex(i, ref prop);

                    hash += CalculateKey(prop.Value);
                }

                return hash;
            }

            if (value is IEnumerable enumerable)
            {
                var hash = 0UL;
                foreach (var item in enumerable)
                {
                    hash += CalculateKey(item);
                }

                return hash;
            }

            if (value is DynamicJsonValue djv)
            {
                var hash = 0UL;
                foreach (var item in djv.Properties)
                {
                    hash += CalculateKey(item.Value);
                }

                return hash;
            }

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
