// -----------------------------------------------------------------------
//  <copyright file="ReplicationTopology.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Database.Bundles.Replication.Data;

namespace Raven.Database.Counters.Replication
{
	public class CountersReplicationTopology
	{
		public CountersReplicationTopology()
		{
			Servers = new HashSet<string>();
			Connections = new HashSet<CountersReplicationTopologyConnection>();
			SkippedResources = new HashSet<string>();
		}

		public HashSet<string> Servers { get; set; }

		public HashSet<CountersReplicationTopologyConnection> Connections { get; set; }

		public HashSet<string> SkippedResources { get; set; }
		public CountersReplicationTopologyConnection GetConnection(string fromUrl, string toUrl)
		{
			return Connections.SingleOrDefault(x => x.Source == fromUrl && x.Destination == toUrl);
		}
	}

	public class CountersReplicationTopologyConnection
	{
		public string Source { get; set; }

		public string Destination { get; set; }

		public Guid SendServerId { get; set; }

		public Guid StoredServerId { get; set; }

		public long LastEtag { get; set; }

		public ReplicatonNodeState SourceToDestinationState { get; set; }

		public ReplicatonNodeState DestinationToSourceState { get; set; }

		public List<string> Errors { get; set; }
	}
}