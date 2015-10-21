namespace Raven.Smuggler.Database.Impl.Remote
{
	public class DatabaseSmugglerRemoteDestinationOptions
	{
		public string ContinuationToken { get; set; }

		public bool WaitForIndexing { get; set; }
	}
}