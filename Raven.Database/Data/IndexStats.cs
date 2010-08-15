using System;

namespace Raven.Database.Data
{
	public class IndexStats
	{
		public string Name { get; set; }
		public int IndexingAttempts { get; set; }
		public int IndexingSuccesses { get; set; }
		public int IndexingErrors { get; set; }

		public Guid LastIndexedEtag { get; set; }
		public DateTime LastIndexedTimestamp { get; set; }
	}
}