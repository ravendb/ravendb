namespace Raven.Abstractions.TimeSeries
{
	public class TimeSeriesKey
	{
		public string Type { get; set; }
		public int NumberOfValues { get; set; }
		public string Key { get; set; }
		public long PointsCount { get; set; }
	}
}