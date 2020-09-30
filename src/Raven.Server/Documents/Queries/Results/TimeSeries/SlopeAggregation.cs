using System;
using System.Collections.Generic;
using System.Diagnostics;
using Raven.Client.Documents.Queries.TimeSeries;
using Raven.Server.Documents.TimeSeries;

namespace Raven.Server.Documents.Queries.Results.TimeSeries
{
    public class SlopeAggregation : TimeSeriesAggregationBase, ITimeSeriesAggregation
    {
        private readonly List<double> _first;

        public SlopeAggregation(string name) : base(AggregationType.Slope, name)
        {
            _first = new List<double>();
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

                if (oldCount <= i)
                {
                    _first[i] = val;
                }

                _values[i] = val; //last
            }
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

                if (i < oldCount)
                {
                    _first[i] = val.First;
                }

                _values[i] = val.Last;
            }
        }

        IEnumerable<double> ITimeSeriesAggregation.GetFinalValues(DateTime? @from, DateTime? to, double? scale = null)
        {
            if (_finalValues != null)
                return _finalValues;

            Debug.Assert(_values.Count == _first.Count, "Invalid slope data");
            Debug.Assert(from.HasValue && to.HasValue, "Missing from/to values");
            
            scale ??= 1;
            var deltaX = (to.Value.Ticks - from.Value.Ticks) / 10_000; // distance in milliseconds
            
            for (int i = 0; i < _values.Count; i++)
            {
                var deltaY = _values[i] - _first[i]; // last - first
                _values[i] = scale.Value * (deltaY / deltaX);
            }

            return _finalValues = _values;
        }

        void ITimeSeriesAggregation.Clear()
        {
            _first.Clear();
            Clear();
        }

        private void InitValuesIfNeeded(int upTo)
        {
            for (int i = _values.Count; i < upTo; i++)
            {
                _first.Add(0);
                _values.Add(0);
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
                if (oldCount <= i)
                {
                    var val = values[index + (int)AggregationType.First];
                    _first[i] = val;
                }

                _values[i] = values[index + (int)AggregationType.Last];
            }
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

                var val = values[index + (int)AggregationType.Last];
                _values[i] += val.Last;

                if (i >= oldCount)
                    continue;

                val = values[index + (int)AggregationType.First];
                _first[i] = val.First;
            }
        }
    }
}
