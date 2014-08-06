using System;

namespace Raven.Abstractions.FileSystem.Notifications
{
	public class CancellationNotification : Notification
	{
		public Guid UploadId { get; set; }
		public string File { get; set; }
	}
}