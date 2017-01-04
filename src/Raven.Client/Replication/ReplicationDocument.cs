//-----------------------------------------------------------------------
// <copyright file="ReplicationDocument.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;

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
    }

    public class ScriptResolver
    {
        public string Script { get; set; }
        public DateTime LastModifiedTime { get; }= DateTime.UtcNow;
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

    public class NewTopology
    {
        public List<NewServerNode> Nodes;
        public NewServerNode LeaderNode;
    }

    public class NewServerNode
    {
        public string Url;
        public string Database;
        public string ApiKey;
        public string CurrentToken;
        public bool IsFailed;

        private const double SwitchBackRatio = 0.75;
        private bool _isRateSurpassed;

        public NewServerNode()
        {
            for (var i = 0; i < 60; i++)
                UpdateRequestTime(0);
        }

        public void UpdateRequestTime(long requestTimeInMilliseconds)
        {
            
        }

        public bool IsRateSurpassed(double requestTimeSlaThresholdInMilliseconds)
        {
            var rate = Rate();

            if (_isRateSurpassed)
                return _isRateSurpassed = rate >= SwitchBackRatio * requestTimeSlaThresholdInMilliseconds;

            return _isRateSurpassed = rate >= requestTimeSlaThresholdInMilliseconds;
        }

        public double Rate()
        {
            return 0;
        }

        private bool Equals(NewServerNode other)
        {
            return string.Equals(Url, other.Url) &&
                string.Equals(Database, other.Database) &&
                string.Equals(ApiKey, other.ApiKey);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((NewServerNode)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = Url?.GetHashCode() ?? 0;
                hashCode = (hashCode * 397) ^ (Database?.GetHashCode() ?? 0);
                hashCode = (hashCode * 397) ^ (ApiKey?.GetHashCode() ?? 0);
                return hashCode;
            }
        }

        private const double MaxDecreasingRatio = 0.75;
        private const double MinDecreasingRatio = 0.25;

        public void DecreaseRate(long requestTimeInMilliseconds)
        {
            var rate = Rate();
            var maxRate = MaxDecreasingRatio * rate;
            var minRate = MinDecreasingRatio * rate;

            var decreasingRate = rate - requestTimeInMilliseconds;

            if (decreasingRate > maxRate)
                decreasingRate = maxRate;

            if (decreasingRate < minRate)
                decreasingRate = minRate;

            UpdateRequestTime((long)decreasingRate);
        }
    }
}
