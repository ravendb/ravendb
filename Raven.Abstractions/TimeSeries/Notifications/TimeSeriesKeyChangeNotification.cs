using System;

namespace Raven.Abstractions.TimeSeries.Notifications
{
	public class KeyChangeNotification : TimeSeriesNotification
	{
		public string Type { get; set; }

		public string Key { get; set; }

		public TimeSeriesChangeAction Action { get; set; }

		public DateTime At { get; set; }
		
		public double[] Values { get; set; }

		public long Start { get; set; }

		public long End { get; set; }
	}

	public class RangeKeyNotification : KeyChangeNotification
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