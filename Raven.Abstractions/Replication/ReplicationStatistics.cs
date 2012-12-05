using System;
using System.Collections.Generic;

namespace Raven.Abstractions.Replication
{
	public class ReplicationStatistics
	{
		public string Self { get; set; }
		public Guid MostRecentDocumentEtag { get; set; }
		public Guid MostRecentAttachmentEtag { get; set; }
		public List<DestinationStats> Stats { get; set; }

		public ReplicationStatistics()
		{
			Stats = new List<DestinationStats>();
		}
	}

	public class DestinationStats
	{
		public int FailureCountInternal = 0;
		public string Url { get; set; }
		public DateTime? LastHeartbeatReceived { get; set; }
		public Guid? LastEtagCheckedForReplication { get; set; }
		public Guid? LastReplicatedEtag { get; set; }
		public DateTime? LastReplicatedLastModified { get; set; }
		public DateTime? LastSuccessTimestamp { get; set; }
		public DateTime? LastFailureTimestamp { get; set; }
		public int FailureCount { get { return FailureCountInternal; } }
		public string LastError { get; set; }
	}
}
