namespace Raven.Client.RavenFS
{
	public class SynchronizationConfirmation
	{
		public string FileName { get; set; }
		public FileStatus Status { get; set; }
	}
}