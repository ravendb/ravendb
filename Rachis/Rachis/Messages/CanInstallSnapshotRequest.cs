namespace Rachis.Messages
{
	public class CanInstallSnapshotRequest : BaseMessage
	{
		public long Index { get; set; }

		public long Term { get; set; }
	}
}