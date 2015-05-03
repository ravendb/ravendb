namespace Raven.Database.Counters
{
	public class ReplicationCounter
	{
		public string FullCounterName { get; set; }

		public long Etag { get; set; }

		public CounterValue CounterValue { get; set; }

		public string GroupName { get { return FullCounterName.Split('/')[0]; } }

		public string CounterName { get { return FullCounterName.Split('/')[1]; } }
	}
}