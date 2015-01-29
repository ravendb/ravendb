using System;

namespace Raven.Database.Counters
{
	public class CounterValue
	{
		public string ServerName { get; set; }
		public Guid ServerId { get; set; }
		public long Value { get; set; }
		public bool IsPositive { get; set; }
	}
}