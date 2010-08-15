using System.Collections.Generic;

namespace Raven.Database.Data
{
	public class DatabaseStatistics
	{
		public int CountOfIndexes { get; set; }

		public long ApproximateTaskCount { get; set; }

		public long CountOfDocuments { get; set; }

		public string[] StaleIndexes { get; set; }

		public IndexStats[] Indexes { get; set; }

		public ServerError[] Errors { get; set; }

		public TriggerInfo[] Triggers { get; set; }

		public class TriggerInfo
		{
			public string Type { get; set; }
			public string Name { get; set; }
		}
	}
}