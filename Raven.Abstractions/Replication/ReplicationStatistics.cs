using System;
using System.Collections.Generic;
using Raven.Abstractions.Data;

namespace Raven.Abstractions.Replication
{
	using Raven.Json.Linq;

	public class ReplicationStatistics
	{
		public string Self { get; set; }
		public Etag MostRecentDocumentEtag { get; set; }
		public Etag MostRecentAttachmentEtag { get; set; }
		public List<DestinationStats> Stats { get; set; }

		public ReplicationStatistics()
		{
			Stats = new List<DestinationStats>();
		}
	}

	public class DestinationStats
	{
		public DestinationStats()
		{
			LastStats = new RavenJArray();
		}

		public int FailureCountInternal = 0;
		public string Url { get; set; }
		public DateTime? LastHeartbeatReceived { get; set; }
		public Etag LastEtagCheckedForReplication { get; set; }
		public Etag LastReplicatedEtag { get; set; }
		public DateTime? LastReplicatedLastModified { get; set; }
		public DateTime? LastSuccessTimestamp { get; set; }
		public DateTime? LastFailureTimestamp { get; set; }
		public int FailureCount { get { return FailureCountInternal; } }
		public string LastError { get; set; }
		public RavenJArray LastStats { get; set; }
	}
}
