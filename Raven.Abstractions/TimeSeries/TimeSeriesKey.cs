namespace Raven.Abstractions.TimeSeries
{
	public class TimeSeriesKey
	{
		public TimeSeriesType Type { get; set; }
		public string Key { get; set; }
		public long PointsCount { get; set; }
	}
}