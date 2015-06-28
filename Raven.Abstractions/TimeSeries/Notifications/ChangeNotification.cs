namespace Raven.Abstractions.TimeSeries.Notifications
{
	public class ChangeNotification : TimeSeriesNotification
	{
		public string GroupName { get; set; }

		public string TimeSeriesName { get; set; }

		public TimeSeriesChangeAction Action { get; set; }

		public long Total { get; set; }
	}

	public enum TimeSeriesChangeAction
	{
		None,
		Add,
		Increment,
		Decrement
	}

	public class StartingWithNotification : ChangeNotification
	{
		
	}

	public class InGroupNotification : ChangeNotification
	{

	}
}