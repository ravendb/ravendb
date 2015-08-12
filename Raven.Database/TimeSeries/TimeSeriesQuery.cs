using System;

namespace Raven.Database.TimeSeries
{
	public class TimeSeriesQuery
	{
		public string Type { get; set; }
		
		public string Key { get; set; }

		public DateTimeOffset Start { get; set; }
		
		public DateTimeOffset End { get; set; }
	}

	public class TimeSeriesRollupQuery : TimeSeriesQuery
	{
		public PeriodDuration Duration { get; set; }
	}
}