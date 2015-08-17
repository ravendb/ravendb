using System;

namespace Raven.Abstractions.TimeSeries
{
	public class TimeSeriesKeySummary
	{
		public TimeSeriesType Type { get; set; }
		public string Key { get; set; }
		public long PointsCount { get; set; }
		public DateTime MinPoint { get; set; }
		public DateTime MaxPoint { get; set; }
	}

	public class TimeSeriesKey
	{
		public TimeSeriesType Type { get; set; }
		public string Key { get; set; }
		public long PointsCount { get; set; }
		public DateTime MinPoint { get; set; }
		public DateTime MaxPoint { get; set; }
	}
}