namespace Raven.Client.RavenFS
{
	public class ConflictNotification : Notification
	{
		public string FileName { get; set; }
        public string SourceServerUrl { get; set; }
        public ConflictStatus Status { get; set; }
	}

    public enum ConflictStatus
    {
        Detected = 0,
        Resolved = 1
    }
}