using System;

namespace Raven.Abstractions.TimeSeries.Notifications
{
	public class TimeSeriesKeyNotification : TimeSeriesNotification
	{
		public string Key { get; set; }

		public TimeSeriesChangeAction Action { get; set; }

		public long At { get; set; }
		
		public double[] Values { get; set; }

		public DateTime Start { get; set; }

		public DateTime End { get; set; }
	}

	public class TimeSeriesRangeKeyNotification : TimeSeriesKeyNotification
	{
		
	}

	public enum TimeSeriesChangeAction
	{
		None,
		Append,
		Delete,
		DeleteInRange
	}
}