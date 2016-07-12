// -----------------------------------------------------------------------
//  <copyright file="ReplicationTopology.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;

using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;

namespace Raven.Database.Bundles.Replication.Data
{
    public class ReplicationTopology
    {
        public ReplicationTopology()
        {
            Servers = new HashSet<string>();
            Connections = new HashSet<ReplicationTopologyConnection>();
            SkippedResources = new HashSet<string>();
            LocalDatabaseIds = new List<Guid>();
        }

        public HashSet<string> Servers { get; set; }

        public HashSet<ReplicationTopologyConnection> Connections { get; set; }

        public HashSet<string> SkippedResources { get; set; }

        public List<Guid> LocalDatabaseIds { get; set; }

        public ReplicationTopologyConnection GetConnection(string fromUrl, string toUrl)
        {
            return Connections.SingleOrDefault(x => x.Source == fromUrl && x.Destination == toUrl);
        }
    }

    public class ReplicationTopologyConnection
    {
        public string Source { get; set; }

        public string Destination { get; set; }

        public Guid SendServerId { get; set; }

        public Guid StoredServerId { get; set; }

        [Obsolete("Use RavenFS instead.")]
        public Etag LastAttachmentEtag { get; set; }

        public Etag LastDocumentEtag { get; set; }

        public TransitiveReplicationOptions ReplicationBehavior { get; set; }

        public ReplicatonNodeState SourceToDestinationState { get; set; }

        public ReplicatonNodeState DestinationToSourceState { get; set; }

        public List<string> Errors { get; set; }
    }
}
