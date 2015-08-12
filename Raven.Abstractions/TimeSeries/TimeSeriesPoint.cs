using System;

namespace Raven.Abstractions.TimeSeries
{
	public class TimeSeriesPointId
	{
		public string Type { get; set; }
		public string Key { get; set; }
		public DateTime At { get; set; }
	}

	public class TimeSeriesPoint
	{
		public DateTime At { get; set; }

		public double[] Values { get; set; }
	}

	public class TimeSeriesFullPoint
	{
		public string Type { get; set; }

		public string Key { get; set; }

		public DateTime At { get; set; }

		public double[] Values { get; set; }
	}
}