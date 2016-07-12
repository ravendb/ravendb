namespace Raven.Abstractions.Replication
{
    public class ReplicationBatchReply
    {
		public enum ReplyType
		{
			None,
			Success,
			Failure
		}

		public ReplyType Type { get; set; }

		public long LastEtagAccepted { get; set; }

		public string Error { get; set; }
    }
}
