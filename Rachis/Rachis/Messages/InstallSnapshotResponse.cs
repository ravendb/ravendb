namespace Rachis.Messages
{
	public class InstallSnapshotResponse : BaseMessage
	{
		public string Message { get; set; }
		public bool Success { get; set; }
		public long CurrentTerm { get; set; }
		public long LastLogIndex { get; set; }

	}
}