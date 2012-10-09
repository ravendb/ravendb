using System;
using System.Collections.Generic;

namespace Raven.Database.Bundles.Replication
{
	public class ReplicationStatistic
	{
		public string Self { get; set; }
		public Guid MostRecentDocumentEtag { get; set; }
		public Guid MostRecentAttachmentEtag { get; set; }
		public List<ReplicationStats> Stats { get; set; }

		public ReplicationStatistic()
		{
			Stats = new List<ReplicationStats>();
		}
	}

	public class ReplicationStats
	{
		public string Url { get; set; }
		public int FailureCount { get; set; }
		public DateTime TimeStamp { get; set; }
	}
}