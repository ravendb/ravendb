using System;

namespace Raven.Client.RavenFS
{
	public class CancellationNotification : Notification
	{
		public Guid UploadId { get; set; }
		public string File { get; set; }
	}
}