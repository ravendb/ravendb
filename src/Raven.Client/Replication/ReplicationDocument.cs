//-----------------------------------------------------------------------
// <copyright file="ReplicationDocument.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using Raven.Client.Cluster;
using Raven.Client.Data;
using Sparrow.Json.Parsing;

namespace Raven.Client.Replication
{
    /// <summary>
    /// This class represent the list of replication destinations for the server
    /// </summary>
    public class ReplicationDocument<TClass>
        where TClass : ReplicationDestination
    {

        public StraightforwardConflictResolution DocumentConflictResolution { get; set; }

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
        public Dictionary<string, ScriptResolver> ResolveByCollection { get; set; }
        public DatabaseResolver DefaultResolver { get; set; }

        public DynamicJsonValue ToJson()
        {
            var json = new DynamicJsonValue
            {
                [nameof(Id)] = Id,
                [nameof(Source)] = Source,
                [nameof(ClientConfiguration)] = ClientConfiguration?.ToJson(),
                [nameof(DocumentConflictResolution)] = DocumentConflictResolution,
                [nameof(DefaultResolver)] = DefaultResolver?.ToJson()
            };

            if (Destinations != null)
            {
                var values = new DynamicJsonArray();
                foreach (var destination in Destinations)
                    values.Add(destination.ToJson());

                json[nameof(Destinations)] = values;
            }

            if (ResolveByCollection != null)
            {
                var values = new DynamicJsonValue();
                foreach (var kvp in ResolveByCollection)
                    values[kvp.Key] = kvp.Value?.ToJson();

                json[nameof(ResolveByCollection)] = values;
            }

            return json;
        }
    }

    public class DatabaseResolver
    {
        public string ResolvingDatabaseId { get; set; }
        public int Version { get; set; } = -1;

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(ResolvingDatabaseId)] = ResolvingDatabaseId,
                [nameof(Version)] = Version
            };
        }
    }

    public class ScriptResolver
    {
        public string Script { get; set; }
        public DateTime LastModifiedTime { get; } = DateTime.UtcNow;

        public object ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Script)] = Script,
                [nameof(LastModifiedTime)] = LastModifiedTime
            };
        }
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
    }
}