using System;
using System.Collections.Generic;
using System.Diagnostics;
using Raven.Client.Documents.Queries.TimeSeries;
using Raven.Server.Documents.TimeSeries;

namespace Raven.Server.Documents.Queries.Results.TimeSeries
{
    public class AverageAggregation : TimeSeriesAggregationBase, ITimeSeriesAggregation
    {
        private readonly List<double> _count;

        public AverageAggregation(string name = null) : base(AggregationType.Average, name)
        {
            _count = new List<double>();
        }

        void ITimeSeriesAggregation.Segment(Span<StatefulTimestampValue> values, bool isRaw)
        {
            if (isRaw == false)
            {
                Debug.Assert(values.Length % 6 == 0);
                var originalNumOfValues = values.Length / 6;

                InitValuesIfNeeded(originalNumOfValues);

                for (int i = 0; i < originalNumOfValues; i++)
                {
                    var index = i * 6;

                    var val = values[index + (int)AggregationType.Sum];
                    _values[i] += val.Sum;
                    
                    val = values[index + (int)AggregationType.Count];
                    _count[i] += (long)val.Sum;
                }

                return;
            }

            InitValuesIfNeeded(values.Length);

            for (int i = 0; i < values.Length; i++)
            {
                var val = values[i];

                _values[i] += val.Sum;
                _count[i] += val.Count;
            }
        }

        void ITimeSeriesAggregation.Step(Span<double> values, bool isRaw)
        {
            if (isRaw == false)
            {
                Debug.Assert(values.Length % 6 == 0);
                var originalNumOfValues = values.Length / 6;

                InitValuesIfNeeded(originalNumOfValues);

                for (int i = 0; i < originalNumOfValues; i++)
                {
                    var index = i * 6;

                    var val = values[index + (int)AggregationType.Sum];
                    _values[i] += val;

                    val = (long)values[index + (int)AggregationType.Count];
                    _count[i] += val;
                }
                return;
            }

            InitValuesIfNeeded(values.Length);

            for (int i = 0; i < values.Length; i++)
            {
                _values[i] += values[i];
                _count[i]++;
            }
        }

        IEnumerable<double> ITimeSeriesAggregation.GetFinalValues(DateTime? @from, DateTime? to, double? scale = null)
        {
            if (_finalValues != null)
                return _finalValues;

            for (int i = 0; i < _values.Count; i++)
            {
                if (_count[i] == 0)
                {
                    _values[i] = double.NaN;
                    continue;
                }

                _values[i] = _values[i] / _count[i];
            }

            return _finalValues = _values;
        }

        void ITimeSeriesAggregation.Clear()
        {
            _count.Clear();
            Clear();
        }

        private void InitValuesIfNeeded(int upTo)
        {
            for (int i = _values.Count; i < upTo; i++)
            {
                _count.Add(0);
                _values.Add(0);
            }
        }
    }
}
