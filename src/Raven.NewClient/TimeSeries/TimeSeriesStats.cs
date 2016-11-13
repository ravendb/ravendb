namespace Raven.Abstractions.TimeSeries
{
    public class TimeSeriesStats
    {
        public string Name { get; set; }

        public string Url { get; set; }

        public string TimeSeriesSize { get; set; }

        public double RequestsPerSecond { get; set; }

        public long TypesCount { get; set; }
        
        public long KeysCount { get; set; }

        public long PointsCount { get; set; }
    }
}
