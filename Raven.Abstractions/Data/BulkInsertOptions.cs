namespace Raven.Abstractions.Data
{
	public class BulkInsertOptions
	{
		public BulkInsertOptions()
		{
			BatchSize = 512;
		}

		public bool OverwriteExisting { get; set; }
		public bool CheckReferencesInIndexes { get; set; }
		public int BatchSize { get; set; }
	}
}