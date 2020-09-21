using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Raven.Client.Documents.Queries.TimeSeries;
using Raven.Server.Documents.TimeSeries;

namespace Raven.Server.Documents.Queries.Results.TimeSeries
{
    public class TimeSeriesAggregation
    {
        public AggregationType Aggregation;

        public bool Any => NumberOfValues > 0;

        public List<double> Count => _count;

        public long TotalCount;

        public string Name;

        public int NumberOfValues => _values.Count;

        private readonly List<double> _values;

        private readonly List<double> _count;

        private IEnumerable<double> _finalValues;

        // slope data
        private readonly List<double> _first; 

        // percentile data
        private double _percentileFactor;
        private List<List<double>> _rankedValues;

        public TimeSeriesAggregation(AggregationType type, string name = null, double? percentile = null)
        {
            Aggregation = type;
            Name = name ?? Aggregation.ToString();

            _count = new List<double>();
            _values = new List<double>();

            if (Aggregation == AggregationType.Percentile)
                InitPercentile(percentile);
            else if (Aggregation == AggregationType.Slope)
                _first = new List<double>();
        }

        public void Init()
        {
            _count.Clear();
            _values.Clear();

            _finalValues = null;
            _rankedValues?.Clear();
            _first?.Clear();
        }

        private void InitPercentile(double? percentile)
        {
            if (percentile.HasValue == false)
                throw new ArgumentException(nameof(percentile));

            if (percentile.Value < 0 || percentile.Value > 100)
                throw new ArgumentOutOfRangeException(
                    $"Invalid argument passed to '{nameof(AggregationType.Percentile)}' aggregation method: '{percentile}'. " +
                    "Argument must be a number between 0 and 100");

            _percentileFactor = percentile.Value / 100;
            _rankedValues = new List<List<double>>();
        }

        public void Segment(Span<StatefulTimestampValue> values, bool isRaw)
        {
            if (isRaw == false)
            {
                SegmentOnRollup(values);
                return;
            }

            if (_count.Count < values.Length)
            {
                for (int i = _count.Count; i < values.Length; i++)
                {
                    _count.Add(0);
                    _values.Add(0);
                    _first?.Add(0);
                }
            }

            for (int i = 0; i < values.Length; i++)
            {
                var val = values[i];
                switch (Aggregation)
                {
                    case AggregationType.Min:
                        if (_count[i] == 0)
                            _values[i] = val.Min;
                        else
                            _values[i] = Math.Min(_values[i], val.Min);
                        break;
                    case AggregationType.Max:
                        if (_count[i] == 0)
                            _values[i] = val.Max;
                        else
                            _values[i] = Math.Max(_values[i], val.Max);
                        break;
                    case AggregationType.Sum:
                    case AggregationType.Average:
                        _values[i] = _values[i] + val.Sum;
                        break;
                    case AggregationType.First:
                        if (_count[i] == 0)
                            _values[i] = val.First;
                        break;
                    case AggregationType.Last:
                        _values[i] = val.Last;
                        break;
                    case AggregationType.Count:
                        break;
                    case AggregationType.Slope:
                        Debug.Assert(_first != null);
                        if (_count[i] == 0)
                            _first[i] = val.First;
                        goto case AggregationType.Last;
                    case AggregationType.Percentile:
                        throw new InvalidOperationException($"Cannot use method '{nameof(Segment)}' on aggregation type '{nameof(AggregationType.Percentile)}' ");
                    default:
                        throw new ArgumentOutOfRangeException("Unknown aggregation operation: " + Aggregation);
                }

                _count[i] += values[i].Count;
            }

            TotalCount += (long)_count[0];
        }

        public void Step(Span<double> values, bool isRaw)
        {
            if (isRaw == false)
            {
                StepOnRollup(values);
                return;
            }

            if (_count.Count < values.Length)
            {
                for (int i = _count.Count; i < values.Length; i++)
                {
                    _count.Add(0);
                    _values.Add(0);

                    _first?.Add(0); 
                    _rankedValues?.Add(new List<double>()); 
                }
            }

            for (int i = 0; i < values.Length; i++)
            {
                var val = values[i];
                switch (Aggregation)
                {
                    case AggregationType.Min:
                        if (_count[i] == 0)
                            _values[i] = val;
                        else
                            _values[i] = Math.Min(_values[i], val);
                        break;
                    case AggregationType.Max:
                        if (_count[i] == 0)
                            _values[i] = val;
                        else
                            _values[i] = Math.Max(_values[i], val);
                        break;
                    case AggregationType.Sum:
                    case AggregationType.Average:
                        _values[i] = _values[i] + val;
                        break;
                    case AggregationType.First:
                        if (_count[i] == 0)
                            _values[i] = val;
                        break;
                    case AggregationType.Last:
                        _values[i] = val;
                        break;
                    case AggregationType.Count:
                        break;
                    case AggregationType.Percentile:
                        Debug.Assert(_rankedValues != null);
                        var currentRankedList = _rankedValues[i];
                        var loc = currentRankedList.BinarySearch(val);
                        currentRankedList.Insert(
                            loc >= 0 ? loc + 1 : ~loc,
                            val);
                        break;
                    case AggregationType.Slope:
                        Debug.Assert(_first != null);
                        if (_count[i] == 0)
                            _first[i] = val;
                        goto case AggregationType.Last;
                    default:
                        throw new ArgumentOutOfRangeException("Unknown aggregation operation: " + Aggregation);
                }

                _count[i]++;
            }

            TotalCount++;
        }

        private void SegmentOnRollup(Span<StatefulTimestampValue> values)
        {
            Debug.Assert(values.Length % 6 == 0);
            var originalNumOfValues = values.Length / 6;
            if (_count.Count < originalNumOfValues)
            {
                for (int i = _count.Count; i < originalNumOfValues; i++)
                {
                    _count.Add(0L);
                    _values.Add(0d);
                    _first?.Add(0);
                }
            }

            for (int i = 0; i < originalNumOfValues; i++)
            {
                var index = i * 6;
                var val = Aggregation == AggregationType.Average
                    ? values[index + (int)AggregationType.Sum]
                    : Aggregation < AggregationType.Average
                            ? values[index + (int)Aggregation]
                            : default;

                switch (Aggregation)
                {
                    case AggregationType.Min:
                        if (_count[i] == 0)
                            _values[i] = val.Min;
                        else
                            _values[i] = Math.Min(_values[i], val.Min);
                        break;
                    case AggregationType.Max:
                        if (_count[i] == 0)
                            _values[i] = val.Max;
                        else
                            _values[i] = Math.Max(_values[i], val.Max);
                        break;
                    case AggregationType.Average:
                    case AggregationType.Sum:
                        _values[i] = _values[i] + val.Sum;
                        break;
                    case AggregationType.First:
                        if (_count[i] == 0)
                            _values[i] = val.First;
                        break;
                    case AggregationType.Last:
                        _values[i] = val.Last;
                        break;
                    case AggregationType.Count:
                        break;
                    case AggregationType.Slope:
                        if (_count[i] == 0)
                        {
                            Debug.Assert(_first != null);
                            val = values[index + (int)AggregationType.First];
                            _first[i] = val.First;
                        }
                        val = values[index + (int)AggregationType.Last];
                        goto case AggregationType.Last;
                    case AggregationType.Percentile:
                        throw new InvalidOperationException($"Cannot use aggregation method '{nameof(AggregationType.Percentile)}' on rolled-up time series");
                    default:
                        throw new ArgumentOutOfRangeException("Unknown aggregation operation: " + Aggregation);
                }

                val = values[index + (int)AggregationType.Count];
                _count[i] += (long)val.Sum;
            }

            TotalCount += (long)_count[0];
        }

        private void StepOnRollup(Span<double> values)
        {
            Debug.Assert(values.Length % 6 == 0);
            var originalNumOfValues = values.Length / 6;
            if (_count.Count < originalNumOfValues)
            {
                for (int i = _count.Count; i < originalNumOfValues; i++)
                {
                    _count.Add(0L);
                    _values.Add(0d);
                    _first?.Add(0);
                }
            }

            for (int i = 0; i < originalNumOfValues; i++)
            {
                var index = i * 6;
                double val;
                switch (Aggregation)
                {
                    case AggregationType.Min:
                        val = values[index + (int)AggregationType.Min];
                        if (_count[i] == 0)
                            _values[i] = val;
                        else
                            _values[i] = Math.Min(_values[i], val);
                        break;
                    case AggregationType.Max:
                        val = values[index + (int)AggregationType.Max];
                        if (_count[i] == 0)
                            _values[i] = val;
                        else
                            _values[i] = Math.Max(_values[i], val);
                        break;
                    case AggregationType.Sum:
                    case AggregationType.Average:
                        val = values[index + (int)AggregationType.Sum];
                        _values[i] = _values[i] + val;
                        break;
                    case AggregationType.First:
                        val = values[index + (int)AggregationType.First];
                        if (_count[i] == 0)
                            _values[i] = val;
                        break;
                    case AggregationType.Last:
                        val = values[index + (int)AggregationType.Last];
                        _values[i] = val;
                        break;
                    case AggregationType.Count:
                        break;
                    case AggregationType.Slope:
                        if (_count[i] == 0)
                        {
                            Debug.Assert(_first != null);
                            _first[i] = values[index + (int)AggregationType.First];
                        }
                        goto case AggregationType.Last;
                    case AggregationType.Percentile:
                        throw new InvalidOperationException($"Cannot use aggregation method '{nameof(AggregationType.Percentile)}' on rolled-up time series");
                    default:
                        throw new ArgumentOutOfRangeException("Unknown aggregation operation: " + Aggregation);
                }

                _count[i] += (long)values[index + (int)AggregationType.Count];
            }

            TotalCount += (long)values[(int)AggregationType.Count];
        }

        public IEnumerable<double> GetFinalValues(DateTime? from, DateTime? to, double? scale = null)
        {
            if (_finalValues != null)
                return _finalValues;

            switch (Aggregation)
            {
                case AggregationType.Min:
                case AggregationType.Max:
                case AggregationType.First:
                case AggregationType.Last:
                case AggregationType.Sum:
                    break;
                case AggregationType.Count:
                    return _count;
                case AggregationType.Average:
                    for (int i = 0; i < _values.Count; i++)
                    {
                        if (_count[i] == 0)
                        {
                            _values[i] = double.NaN;
                            continue;
                        }

                        _values[i] = _values[i] / _count[i];
                    }
                    break;
                case AggregationType.Percentile:
                    return _finalValues = GetPercentile(scale ?? 1);
                case AggregationType.Slope:
                    Debug.Assert(_values.Count == _first.Count, "Invalid aggregation data");
                    Debug.Assert(from.HasValue && to.HasValue, "Missing from/to");

                    var deltaX = (to.Value.Ticks - from.Value.Ticks) / 10_000; // offset in milliseconds
                    for (int i = 0; i < _values.Count; i++)
                    {
                        var deltaY = _values[i] - _first[i]; // last - first
                        _values[i] = deltaY / deltaX;
                    }
                    break;
                default:
                    throw new ArgumentOutOfRangeException("Unknown aggregation operation: " + Aggregation);
            }

            if (scale.HasValue == false) 
                return _finalValues = _values;

            return _finalValues = _values.Select(x => x * scale.Value);
        }

        private IEnumerable<double> GetPercentile(double scale)
        {
            foreach (var list in _rankedValues)
            {
                var index = (int)Math.Ceiling(_percentileFactor * list.Count);

                yield return scale * list[index - 1];
            }
        }
    }
}
