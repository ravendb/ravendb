namespace Raven.Studio.Messages
{
	using System;

	public class Notification
	{
		public Notification(string message, NotificationLevel level= NotificationLevel.Warning)
		{
			Message = message;
			Level = level;
			CreatedAt = DateTime.Now;
		}

		public DateTime CreatedAt { get; private set; }
		public string Message { get; private set; }
		public NotificationLevel Level { get; private set; }
	}

	public enum NotificationLevel
	{
		Info,
		Warning,
		Error,
	}
}