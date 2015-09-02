using System;

namespace Raven.Database.TimeSeries
{
	public class TimeSeriesRollupQuery
	{
		public string Type { get; set; }

		public string Key { get; set; }

		public DateTimeOffset Start { get; set; }

		public DateTimeOffset End { get; set; }

		public PeriodDuration Duration { get; set; }
	}
}