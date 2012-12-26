namespace Raven.Abstractions.Data
{
	public class BulkInsertOptions
	{
		public BulkInsertOptions()
		{
			BatchSize = 512;
		}

		public bool CheckForUpdates { get; set; }
		public bool CheckReferencesInIndexes { get; set; }
		public int BatchSize { get; set; }
	}
}