namespace Raven.Abstractions.Replication
{
    public class ReplicationBatchReply
    {
		public enum ReplyType
		{
			Success,
			Failure
		}

		public ReplyType Type { get; set; }

		public long LastEtagAccepted { get; set; }

		public string Error { get; set; }
    }
}
