namespace Raven.Abstractions.Counters.Notifications
{
	public class ChangeNotification : CounterStorageNotification
	{
		public string GroupName { get; set; }

		public string CounterName { get; set; }

		public CounterChangeAction Action { get; set; }

		public long Delta { get; set; }

		public long Total { get; set; }
	}

	public enum CounterChangeAction
	{
		Add,
		Increment,
		Decrement,
		Delete
	}

	public class StartingWithNotification : ChangeNotification
	{
		
	}

	public class InGroupNotification : ChangeNotification
	{

	}
}