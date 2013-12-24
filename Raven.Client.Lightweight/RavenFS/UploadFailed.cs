using System;

namespace Raven.Client.RavenFS
{
	public class UploadFailed : Notification
	{
		public Guid UploadId { get; set; }
		public string File { get; set; }
	}
}