namespace Raven.Abstractions.TimeSeries
{
    public class TimeSeriesStats
    {
        public string Name { get; set; }

        public string Url { get; set; }

        public long TimeSeriesCount { get; set; }

        public string TimeSeriesSize { get; set; }

		public double RequestsPerSecond { get; set; }
    }
}