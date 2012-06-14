using System;
using Raven.Abstractions;

namespace Raven.Studio.Messages
{
	public class Notification
	{
		public Notification(string message, NotificationLevel level = NotificationLevel.Info, Exception exception = null)
		{
			Message = message;
			Level = level;
			CreatedAt = SystemTime.Now;
		}

		public DateTime CreatedAt { get; private set; }
		public string Message { get; private set; }
		public NotificationLevel Level { get; private set; }
	}
}