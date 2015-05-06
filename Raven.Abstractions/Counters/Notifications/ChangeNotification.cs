namespace Raven.Abstractions.Counters.Notifications
{
	public class ChangeNotification : CounterStorageNotification
	{
		public string GroupName { get; set; }

		public string CounterName { get; set; }

		public CounterChangeAction Action { get; set; }
	}

	public enum CounterChangeAction
	{
		None,
		Add,
		Increment,
		Decrement
	}

	public class LocalChangeNotification : ChangeNotification
	{
		
	}

	public class ReplicationChangeNotification : ChangeNotification
	{

	}
}