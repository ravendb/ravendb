namespace Raven.Client.RavenFS
{
	public class ConflictDetected : ConflictNotification
	{
		public string SourceServerUrl { get; set; }
	}
}