// -----------------------------------------------------------------------
//  <copyright file="ReplicationData.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Raven.Json.Linq;

namespace Raven.Database.Bundles.Replication.Impl
{
    public class ReplicationData
    {
        public static RavenJArray GetOrCreateHistory(RavenJObject metadata)
        {
            return (metadata[Constants.RavenReplicationHistory] as RavenJArray) ?? new RavenJArray();
        }

        public static RavenJArray GetHistory(RavenJObject metadata)
        {
            var currentHistory = metadata[Constants.RavenReplicationHistory] as RavenJArray;
            if (currentHistory != null)
                return currentHistory;

            var replicationVersion = metadata[Constants.RavenReplicationVersion];
            var replicationSource = metadata[Constants.RavenReplicationSource];

            if (replicationVersion == null || replicationSource == null)
                return new RavenJArray();

            return new RavenJArray
            {
                new RavenJObject
                {
                    {Constants.RavenReplicationVersion, replicationVersion},
                    {Constants.RavenReplicationSource, replicationSource}
                }
            };
        }

        public static void SetHistory(RavenJObject metadata, RavenJArray history)
        {
            metadata[Constants.RavenReplicationHistory] = history;
        }
    }
}
