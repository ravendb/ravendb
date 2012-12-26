namespace Raven.Abstractions.Data
{
	public class BulkInsertOptions
	{
		public bool CheckForUpdates { get; set; }
		public bool CheckReferencesInIndexes { get; set; }
	}
}