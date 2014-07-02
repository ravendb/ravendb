using System;

namespace Raven.Abstractions.Data
{
	public class BulkInsertOptions
	{
		public BulkInsertOptions()
		{
			BatchSize = 512;
		    WriteTimeoutMilliseconds = 15*1000;
		}

		public bool OverwriteExisting { get; set; }
		public bool CheckReferencesInIndexes { get; set; }
		
		public int BatchSize { get; set; }

		public int WriteTimeoutMilliseconds { get; set; }
	}
}