using System;
using System.Collections.Generic;
using System.Diagnostics;
using Raven.Client.Documents.Queries.TimeSeries;
using Raven.Server.Documents.TimeSeries;

namespace Raven.Server.Documents.Queries.Results.TimeSeries
{
    public class PercentileAggregation : TimeSeriesAggregationBase, ITimeSeriesAggregation
    {
        private readonly double _percentileFactor;
        private readonly List<SortedDictionary<double, int>> _rankedValues;
        private readonly List<long> _count;

        public PercentileAggregation(string name = null, double? percentile = null) : base(AggregationType.Percentile, name)
        {
            if (percentile.HasValue == false)
                throw new ArgumentException(nameof(percentile));

            if (percentile.Value <= 0 || percentile.Value > 100)
                throw new ArgumentOutOfRangeException(
                    $"Invalid argument passed to '{nameof(AggregationType.Percentile)}' aggregation method: '{percentile}'. " +
                    "Argument must be a number between 0 and 100");

            _percentileFactor = percentile.Value / 100;
            _rankedValues = new List<SortedDictionary<double, int>>();
            _count = new List<long>();
        }

        void ITimeSeriesAggregation.Clear()
        {
            _rankedValues.Clear();
            _count.Clear();
            Clear();
        }

        void ITimeSeriesAggregation.Segment(Span<StatefulTimestampValue> values, bool isRaw)
        {
            throw new InvalidOperationException($"Cannot use method '{nameof(ITimeSeriesAggregation.Segment)}' on aggregation type '{nameof(AggregationType.Percentile)}' ");
        }

        void ITimeSeriesAggregation.Step(Span<double> values, bool isRaw)
        {
            if (isRaw == false)
                throw new InvalidOperationException($"Cannot use aggregation method '{nameof(AggregationType.Percentile)}' on rolled-up time series");

            if (_rankedValues.Count < values.Length)
            {
                for (int i = _rankedValues.Count; i < values.Length; i++)
                {
                    _rankedValues.Add(new SortedDictionary<double, int>()); 
                    _count.Add(0);
                }
            }

            for (int i = 0; i < values.Length; i++)
            {
                var val = values[i];
                var sortedDict = _rankedValues[i];
                sortedDict.TryGetValue(val, out int valCount);
                sortedDict[val] = valCount + 1;
                _count[i]++;
            }
        }

        IEnumerable<double> ITimeSeriesAggregation.GetFinalValues(DateTime? from, DateTime? to, double? scale)
        {
            if (_finalValues != null)
                return _finalValues;

            return _finalValues = GetPercentile(scale ?? 1);
        }

        private IEnumerable<double> GetPercentile(double scale)
        {
            for (var i = 0; i < _rankedValues.Count; i++)
            {
                var sortedDict= _rankedValues[i];
                var itemsCount = _count[i];
                var rank = (int)Math.Ceiling(_percentileFactor * itemsCount);

                var count = 0;
                double? result = null;
                foreach ((double val, int valCount) in sortedDict)
                {
                    count += valCount;
                    
                    if (rank != count && count != itemsCount) 
                        continue;
                    
                    result = val;
                    break;
                }

                Debug.Assert(result.HasValue);

                yield return scale * result.Value;
            }
        }
    }
}
