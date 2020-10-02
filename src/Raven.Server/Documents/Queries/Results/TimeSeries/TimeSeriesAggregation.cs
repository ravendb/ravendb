using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Raven.Client.Documents.Queries.TimeSeries;
using Raven.Server.Documents.TimeSeries;

namespace Raven.Server.Documents.Queries.Results.TimeSeries
{
    public interface ITimeSeriesAggregation
    {
        public void Segment(Span<StatefulTimestampValue> values, bool isRaw);

        public void Step(Span<double> values, bool isRaw);

        public IEnumerable<double> GetFinalValues(DateTime? from, DateTime? to, double? scale = null);

        public void Clear();

        public bool Any { get; }

        public string Name { get; }

        public int NumberOfValues { get; }

        public AggregationType Aggregation { get; }

    }

    public abstract class TimeSeriesAggregationBase
    {
        public AggregationType Aggregation { get; }

        public string Name { get; }

        public bool Any => NumberOfValues > 0;

        public int NumberOfValues => _values.Count;

        protected readonly List<double> _values;

        protected IEnumerable<double> _finalValues;

        protected TimeSeriesAggregationBase(AggregationType type, string name)
        {
            Aggregation = type;
            Name = name ?? Aggregation.ToString();

            _values = new List<double>();
        }

        public void Clear()
        {
            _values.Clear();
            _finalValues = null;
        }
    }

    public class TimeSeriesAggregation : TimeSeriesAggregationBase, ITimeSeriesAggregation
    {
        public TimeSeriesAggregation(AggregationType type, string name = null) : base(type, name)
        {
            if (type >= AggregationType.Average)
                throw new ArgumentException(nameof(type));
        }

        void ITimeSeriesAggregation.Segment(Span<StatefulTimestampValue> values, bool isRaw)
        {
            if (isRaw == false)
            {
                SegmentOnRollup(values);
                return;
            }

            var oldCount = _values.Count;
            InitValuesIfNeeded(values.Length);

            for (int i = 0; i < values.Length; i++)
            {
                var val = values[i];
                var first = oldCount <= i;

                switch (Aggregation)
                {
                    case AggregationType.Min:
                        if (first)
                            _values[i] = val.Min;
                        else
                            _values[i] = Math.Min(_values[i], val.Min);
                        break;
                    case AggregationType.Max:
                        if (first)
                            _values[i] = val.Max;
                        else
                            _values[i] = Math.Max(_values[i], val.Max);
                        break;
                    case AggregationType.Sum:
                        _values[i] += val.Sum;
                        break;
                    case AggregationType.First:
                        if (first)
                            _values[i] = val.First;
                        break;
                    case AggregationType.Last:
                        _values[i] = val.Last;
                        break;
                    case AggregationType.Count:
                        _values[i] += val.Count;
                        break;
                    case AggregationType.Percentile:
                        throw new InvalidOperationException($"Cannot use method '{nameof(ITimeSeriesAggregation.Segment)}' on aggregation type '{nameof(AggregationType.Percentile)}' ");
                    default:
                        throw new ArgumentOutOfRangeException("Unknown aggregation operation: " + Aggregation);
                }
            }
        }

        void ITimeSeriesAggregation.Step(Span<double> values, bool isRaw)
        {
            if (isRaw == false)
            {
                StepOnRollup(values);
                return;
            }

            var oldCount = _values.Count;
            InitValuesIfNeeded(values.Length);

            for (int i = 0; i < values.Length; i++)
            {
                var val = values[i];
                var first = oldCount <= i;

                switch (Aggregation)
                {
                    case AggregationType.Min:
                        if (first)
                            _values[i] = val;
                        else
                            _values[i] = Math.Min(_values[i], val);
                        break;
                    case AggregationType.Max:
                        if (first)
                            _values[i] = val;
                        else
                            _values[i] = Math.Max(_values[i], val);
                        break;
                    case AggregationType.Sum:
                        _values[i] += val;
                        break;
                    case AggregationType.First:
                        if (first)
                            _values[i] = val;
                        break;
                    case AggregationType.Last:
                        _values[i] = val;
                        break;
                    case AggregationType.Count:
                        _values[i]++;
                        break;
                    default:
                        throw new ArgumentOutOfRangeException("Unknown aggregation operation: " + Aggregation);
                }
            }
        }

        IEnumerable<double> ITimeSeriesAggregation.GetFinalValues(DateTime? from, DateTime? to, double? scale)
        {
            if (_finalValues != null)
                return _finalValues;

            if (scale.HasValue == false || Aggregation == AggregationType.Count)
                return _finalValues = _values;

            return _finalValues = _values.Select(x => x * scale.Value);
        }

        private void SegmentOnRollup(Span<StatefulTimestampValue> values)
        {
            Debug.Assert(values.Length % 6 == 0);
            var originalNumOfValues = values.Length / 6;
            var oldCount = _values.Count;
            InitValuesIfNeeded(originalNumOfValues);

            for (int i = 0; i < originalNumOfValues; i++)
            {
                var index = i * 6;
                var val = values[index + (int)Aggregation];

                var first = oldCount <= i;
                switch (Aggregation)
                {
                    case AggregationType.Min:
                        if (first)
                            _values[i] = val.Min;
                        else
                            _values[i] = Math.Min(_values[i], val.Min);
                        break;
                    case AggregationType.Max:
                        if (first)
                            _values[i] = val.Max;
                        else
                            _values[i] = Math.Max(_values[i], val.Max);
                        break;
                    case AggregationType.Sum:
                    case AggregationType.Count:
                        _values[i] += val.Sum;
                        break;
                    case AggregationType.First:
                        if (first)
                            _values[i] = val.First;
                        break;
                    case AggregationType.Last:
                        _values[i] = val.Last;
                        break;
                    default:
                        throw new ArgumentOutOfRangeException("Unknown aggregation operation: " + Aggregation);
                }
            }
        }

        private void StepOnRollup(Span<double> values)
        {
            Debug.Assert(values.Length % 6 == 0);
            var originalNumOfValues = values.Length / 6;
            var oldCount = _values.Count;
            InitValuesIfNeeded(originalNumOfValues);

            for (int i = 0; i < originalNumOfValues; i++)
            {
                var index = i * 6;
                var val = values[index + (int)Aggregation];
                var first = oldCount <= i;

                switch (Aggregation)
                {
                    case AggregationType.Min:
                        if (first)
                            _values[i] = val;
                        else
                            _values[i] = Math.Min(_values[i], val);
                        break;
                    case AggregationType.Max:
                        if (first)
                            _values[i] = val;
                        else
                            _values[i] = Math.Max(_values[i], val);
                        break;
                    case AggregationType.Sum:
                    case AggregationType.Count:
                        _values[i] += val;
                        break;
                    case AggregationType.First:
                        if (first)
                            _values[i] = val;
                        break;
                    case AggregationType.Last:
                        _values[i] = val;
                        break;
                    default:
                        throw new ArgumentOutOfRangeException("Unknown aggregation operation: " + Aggregation);
                }
            }
        }

        private void InitValuesIfNeeded(int upTo)
        {
            for (int i = _values.Count; i < upTo; i++)
            {
                _values.Add(0);
            }
        }
    }
}
