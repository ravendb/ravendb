// -----------------------------------------------------------------------
//  <copyright file="SynchronizationTopology.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Database.Bundles.Replication.Data;

namespace Raven.Database.FileSystem.Synchronization
{
    public class SynchronizationTopology
    {
        public SynchronizationTopology()
        {
            Servers = new HashSet<string>();
            Connections = new HashSet<SynchronizationTopologyConnection>();
            SkippedResources = new HashSet<string>();
        }

        public HashSet<string> Servers { get; set; }

        public HashSet<SynchronizationTopologyConnection> Connections { get; set; }

        public HashSet<string> SkippedResources { get; set; }

        public SynchronizationTopologyConnection GetConnection(Guid fromServerId, Guid? toServerId, string fromUrl, string toUrl)
        {
            if (toServerId.HasValue)
            {
                //try to match by server ids
                return Connections
                    .SingleOrDefault(x => x.SourceServerId == fromServerId
                                          && x.DestinationServerId == toServerId);
            }

            return Connections
                .SingleOrDefault(x => x.SourceUrl.Any(y => y == fromUrl)
                                      && x.DestinationUrl.Any(y => y == toUrl));
        }
    }

    public class SynchronizationTopologyConnection
    {
        public SynchronizationTopologyConnection()
        {
            SourceUrl = new HashSet<string>();
            DestinationUrl = new HashSet<string>();
        }

        public HashSet<string> SourceUrl { get; set; }

        public HashSet<string> DestinationUrl { get; set; }

        public Guid SourceServerId => SendServerId == Guid.Empty ? StoredServerId : SendServerId;

        public Guid? DestinationServerId { get; set; }

        public Guid SendServerId { get; set; }

        public Guid StoredServerId { get; set; }

        public Etag LastSourceFileEtag { get; set; }

        public ReplicatonNodeState SourceToDestinationState { get; set; }

        public ReplicatonNodeState DestinationToSourceState { get; set; }

        public List<string> Errors { get; set; }

        //left for backward compatibility with v3.0
        public string Source { get; set; }

        public string Destination { get; set; }
    }
}
