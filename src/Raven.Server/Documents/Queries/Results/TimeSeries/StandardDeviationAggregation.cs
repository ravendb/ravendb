using System;
using System.Collections.Generic;
using System.Diagnostics;
using Raven.Client.Documents.Queries.TimeSeries;
using Raven.Server.Documents.TimeSeries;

namespace Raven.Server.Documents.Queries.Results.TimeSeries
{
    public class StandardDeviationAggregation : TimeSeriesAggregationBase, ITimeSeriesAggregation
    {
        private readonly List<List<double>> _allValues;

        public StandardDeviationAggregation(string name) : base(AggregationType.StandardDeviation, name)
        {
            _allValues = new List<List<double>>();
        }

        void ITimeSeriesAggregation.Segment(Span<StatefulTimestampValue> values, bool isRaw)
        {
            throw new InvalidOperationException($"Cannot use method '{nameof(ITimeSeriesAggregation.Segment)}' on aggregation type '{nameof(AggregationType.StandardDeviation)}' ");
        }

        void ITimeSeriesAggregation.Step(Span<double> values, bool isRaw)
        {
            if (isRaw == false)
                throw new InvalidOperationException($"Cannot use aggregation method '{nameof(AggregationType.StandardDeviation)}' on rolled-up time series");
            
            InitValuesIfNeeded(values.Length);

            for (int i = 0; i < values.Length; i++)
            {
                var val = values[i];

                _values[i] += val; // sum
                _allValues[i].Add(val);
            }
        }

        IEnumerable<double> ITimeSeriesAggregation.GetFinalValues(DateTime? @from, DateTime? to, double? scale)
        {
            if (_finalValues != null)
                return _finalValues;

            // calculate mean values 
            for (int i = 0; i < _values.Count; i++)
            {
                var count = _allValues[i].Count;
                Debug.Assert(count > 0, "Invalid aggregation data");

                _values[i] = _values[i] / count;
            }

            scale ??= 1;
            for (int i = 0; i < _allValues.Count; i++)
            {
                var mean = _values[i];
                var current = _allValues[i];
                _values[i] = 0;

                // calculate the sum of squared differences from mean 
                foreach (var value in current)
                {
                    var diff = scale.Value * (value - mean);
                    // use _values to sum the differences
                    _values[i] += diff * diff;
                }

                // calculate the squared root of Σ / N-1
                // use _values to keep the final results
                _values[i] = Math.Sqrt(_values[i] / (current.Count - 1));
            }

            return _finalValues = _values;
        }

        void ITimeSeriesAggregation.Clear()
        {
            _allValues.Clear();
            Clear();
        }

        private void InitValuesIfNeeded(int upTo)
        {
            for (int i = _values.Count; i < upTo; i++)
            {
                _values.Add(0);
                _allValues.Add(new List<double>());
            }
        }
    }
}
