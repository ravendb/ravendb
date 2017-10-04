//-----------------------------------------------------------------------
// <copyright file="ReplicationDocument.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Cluster;
using Raven.Abstractions.Data;

namespace Raven.Abstractions.Replication
{
    /// <summary>
    /// This class represent the list of replication destinations for the server
    /// </summary>
    public class ReplicationDocument<TClass>
        where TClass : ReplicationDestination
    {
        /// <summary>
        /// Gets or sets the list of replication destinations.
        /// </summary>
        public List<TClass> Destinations { get; set; }

        /// <summary>
        /// Gets or sets the id.
        /// </summary>
        /// <value>The id.</value>
        public string Id { get; set; }

        /// <summary>
        /// Gets or sets the Source.
        /// </summary>
        /// <value>The Source.</value>
        public string Source { get; set; }

        /// <summary>
        /// Configuration for clients.
        /// </summary>
        public ReplicationClientConfiguration ClientConfiguration { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="ReplicationDocument"/> class.
        /// </summary>
        public ReplicationDocument()
        {
            Id = Constants.RavenReplicationDestinations;
            Destinations = new List<TClass>();
        }
    }

    /// <summary>
    /// This class represent the list of replication destinations for the server
    /// </summary>
    public class ReplicationDocument : ReplicationDocument<ReplicationDestination>
    {
    }

    public class ReplicationDocumentWithClusterInformation : ReplicationDocument<ReplicationDestination.ReplicationDestinationWithClusterInformation>
    {
        public ReplicationDocumentWithClusterInformation()
        {
            ClusterInformation = new ClusterInformation(false, false);
            ClusterCommitIndex = -1;
            Term = -1;
        }

        public ClusterInformation ClusterInformation { get; set; }
        public long Term { get; set; }
        public long ClusterCommitIndex { get; set; }
        public bool HasLeader
        {
            get { return ClusterInformation.IsLeader || Destinations.Any(d => d.ClusterInformation.IsLeader); }
        }

        public override string ToString()
        {
            return $"ClusterInformation=[{ClusterInformation}] " +
                   $"ClusterCommitIndex={ClusterCommitIndex} Term={Term} " +
                   $"Source={Source} ";
        }
    }
}
