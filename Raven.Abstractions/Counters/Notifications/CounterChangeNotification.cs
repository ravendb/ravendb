using System;

namespace Raven.Abstractions.Counters.Notifications
{
	public class CounterChangeNotification : CounterStorageNotification
	{
		public string GroupName { get; set; }

		public string CounterName { get; set; }

		public CounterChangeAction Action { get; set; }

		public CounterChangeType Type { get; set; }
	}

	public enum CounterChangeAction
	{
		None,
		Add,
		Increment,
		Decrement
	}

	[Flags]
	public enum CounterChangeType
	{
		All = 0,
		Local = 1,
		Replication = 2
	}
}