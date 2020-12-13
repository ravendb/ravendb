namespace Raven.Client.Documents.Queries.TimeSeries
{
    public class TimeSeriesAggregationOptions
    {
        public InterpolationType Interpolation { get; set; }
    }

    public enum InterpolationType
    {
        None,
        Linear,
        Nearest,
        Last,
        Next
    }
}
