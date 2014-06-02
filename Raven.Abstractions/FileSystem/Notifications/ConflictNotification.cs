namespace Raven.Abstractions.FileSystem.Notifications
{
	public class ConflictNotification : Notification
	{
		public string FileName { get; set; }
	}
}