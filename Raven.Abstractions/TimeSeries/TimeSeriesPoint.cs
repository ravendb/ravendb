using System;

namespace Raven.Abstractions.TimeSeries
{
	public class TimeSeriesPoint
	{
		public DateTime At { get; set; }

		public double[] Values { get; set; }
	}
}