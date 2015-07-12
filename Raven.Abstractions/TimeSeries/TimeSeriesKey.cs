namespace Raven.Abstractions.TimeSeries
{
	public class TimeSeriesKey
	{
		public string Prefix { get; set; }
		public byte ValueLength { get; set; }
		public string Key { get; set; }
		public long PointsCount { get; set; }
	}
}