namespace Raven.Client.RavenFS
{
	public class ConflictDetectedNotification : ConflictNotification
	{
		public string SourceServerUrl { get; set; }
	}
}