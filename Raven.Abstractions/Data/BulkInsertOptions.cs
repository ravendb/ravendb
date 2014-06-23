using System;

namespace Raven.Abstractions.Data
{
	public class BulkInsertOptions
	{
		public BulkInsertOptions()
		{
			BatchSize = 512;
			FlushingTimeout = TimeSpan.FromMinutes(5);
		}

		public bool OverwriteExisting { get; set; }
		public bool CheckReferencesInIndexes { get; set; }
		public int BatchSize { get; set; }

		public TimeSpan FlushingTimeout { get; set; }
	}
}