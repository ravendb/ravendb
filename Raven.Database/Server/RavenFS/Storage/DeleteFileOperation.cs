namespace Raven.Database.Server.RavenFS.Storage
{
	public class DeleteFileOperation
	{
		public string OriginalFileName { get; set; }

		public string CurrentFileName { get; set; }
	}
}