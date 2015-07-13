namespace Raven.Abstractions.TimeSeries
{
	public class TimeSeriesPoint
	{
		public long At { get; set; }
		public double[] Values { get; set; }
	}
}