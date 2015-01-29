namespace Raven.Database.Counters
{
	public class ReplicationCounter
	{
		public string FullCounterName { get; set; }
		public long Etag { get; set; }
		public CounterValue CounterValue { get; set; }
	}
}