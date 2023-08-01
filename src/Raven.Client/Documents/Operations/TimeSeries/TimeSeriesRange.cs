using System;
using Sparrow;

namespace Raven.Client.Documents.Operations.TimeSeries
{
    public sealed class TimeSeriesRange : AbstractTimeSeriesRange
    {
        public DateTime? From, To;
    }

    internal sealed class TimeSeriesTimeRange : AbstractTimeSeriesRange
    {
        public TimeValue Time;
        public TimeSeriesRangeType Type;
    }

    internal sealed class TimeSeriesCountRange : AbstractTimeSeriesRange
    {
        public int Count;
        public TimeSeriesRangeType Type;
    }

    public abstract class AbstractTimeSeriesRange
    {
        public string Name;
    }

    public enum TimeSeriesRangeType
    {
        None,
        Last
    }
}
