namespace Rachis.Messages
{
	public class CanInstallSnapshotResponse : BaseMessage
	{
		public bool IsCurrentlyInstalling { get; set; }
		public long Term { get; set; }
		public long Index { get; set; }
		public bool Success { get; set; }
		public string Message { get; set; }
	}
}