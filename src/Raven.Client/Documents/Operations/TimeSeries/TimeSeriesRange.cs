using System;
using Sparrow;

namespace Raven.Client.Documents.Operations.TimeSeries
{
    /// <summary>
    /// Represents a range of time series data based on specific start and end times.
    /// </summary>
    public sealed class TimeSeriesRange : AbstractTimeSeriesRange
    {
        /// <summary>
        /// The start time of the range.
        /// Data points from this timestamp (inclusive) will be included in the range.
        /// If <c>null</c>, the range starts from the beginning of the time series.
        /// </summary>
        public DateTime? From;

        /// <summary>
        /// The end time of the range.
        /// Data points up to this timestamp (inclusive) will be included in the range.
        /// If <c>null</c>, the range extends to the end of the time series.
        /// </summary>
        public DateTime? To;
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

    /// <summary>
    /// Represents a base class for defining time series ranges.
    /// </summary>
    public abstract class AbstractTimeSeriesRange
    {
        /// <summary>
        /// The name of the time series.
        /// </summary>
        /// <remarks>
        /// This field identifies the time series on which the range operations will be performed.
        /// </remarks>
        public string Name;
    }

    /// <summary>
    /// Specifies the type of time series range operation.
    /// </summary>
    public enum TimeSeriesRangeType
    {
        /// <summary>
        /// No range type specified.
        /// </summary>
        None,

        /// <summary>
        /// Specifies a range that retrieves the last set of data points from the time series.
        /// </summary>
        Last
    }
}
