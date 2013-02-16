using System.Collections.Generic;

namespace Raven.Database.Indexing
{
	public class IndexingWorkStats
	{
		public int IndexingAttempts;
		public int IndexingSuccesses;
		public int IndexingErrors;

		public int ReduceAttempts;
		public int ReduceSuccesses;
		public int ReduceErrors;

		public Status Operation;

		public IndexingWorkStats(IEnumerable<IndexingWorkStats> stream)
		{
			foreach (var other in stream)
			{
				IndexingAttempts += other.IndexingAttempts;
				IndexingSuccesses += other.IndexingSuccesses;
				IndexingErrors += other.IndexingErrors;
				ReduceAttempts += other.ReduceAttempts;
				ReduceSuccesses += other.ReduceSuccesses;
				ReduceErrors += other.ReduceErrors;
				Operation = other.Operation;
			}
		}

		public IndexingWorkStats()
		{
			
		}

		public enum Status
		{
			Map,
			Reduce,
			Ignore
		}
	}
}