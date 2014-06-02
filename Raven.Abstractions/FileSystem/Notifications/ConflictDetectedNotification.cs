namespace Raven.Abstractions.FileSystem.Notifications
{
	public class ConflictDetectedNotification : ConflictNotification
	{
		public string SourceServerUrl { get; set; }
	}
}