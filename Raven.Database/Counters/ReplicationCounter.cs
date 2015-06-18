using System;

namespace Raven.Database.Counters
{
	public class ReplicationCounter
	{
		public string GroupName { get; set; }

		public string CounterName { get; set; }

		public Guid ServerId { get; set; }

		public long Value { get; set; }

		public long Etag { get; set; }
	}
}