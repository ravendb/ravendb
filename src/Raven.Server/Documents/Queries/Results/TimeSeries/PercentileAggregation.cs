using System;
using System.Collections.Generic;
using Raven.Client.Documents.Queries.TimeSeries;
using Raven.Server.Documents.TimeSeries;

namespace Raven.Server.Documents.Queries.Results.TimeSeries
{
    public class PercentileAggregation : TimeSeriesAggregationBase, ITimeSeriesAggregation
    {
        private readonly double _percentileFactor;
        private readonly List<List<double>> _rankedValues;

        public PercentileAggregation(string name = null, double? percentile = null) : base(AggregationType.Percentile, name)
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

        void ITimeSeriesAggregation.Clear()
        {
            _rankedValues.Clear();
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
                    _rankedValues.Add(new List<double>()); 
                }
            }

            for (int i = 0; i < values.Length; i++)
            {
                var val = values[i];
                var currentRankedList = _rankedValues[i];
                var loc = currentRankedList.BinarySearch(val);
                currentRankedList.Insert(
                    loc >= 0 ? loc + 1 : ~loc,
                    val);
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
            foreach (var list in _rankedValues)
            {
                var index = (int)Math.Ceiling(_percentileFactor * list.Count);

                yield return scale * list[index - 1];
            }
        }
    }
}
