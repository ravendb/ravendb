namespace Raven.Abstractions.TimeSeries
{
    public class TimeSeriesType
    {
        public string Type { get; set; }

        public string[] Fields { get; set; }
        
        public long KeysCount { get; set; }
    }
}
