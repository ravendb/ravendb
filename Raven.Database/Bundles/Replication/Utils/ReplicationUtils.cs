// -----------------------------------------------------------------------
//  <copyright file="ReplicationUtils.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Linq;

using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Bundles.Replication.Tasks;

namespace Raven.Database.Bundles.Replication.Utils
{
    public static class ReplicationUtils
    {
        internal static ReplicationStatistics GetReplicationInformation(DocumentDatabase database)
        {
            var mostRecentDocumentEtag = Etag.Empty;
            var mostRecentAttachmentEtag = Etag.Empty;
            database.TransactionalStorage.Batch(accessor =>
            {
                mostRecentDocumentEtag = accessor.Staleness.GetMostRecentDocumentEtag();
                mostRecentAttachmentEtag = accessor.Staleness.GetMostRecentAttachmentEtag();
            });

            var replicationTask = database.StartupTasks.OfType<ReplicationTask>().FirstOrDefault();
            var replicationStatistics = new ReplicationStatistics
            {
                Self = database.ServerUrl,
                MostRecentDocumentEtag = mostRecentDocumentEtag,
                MostRecentAttachmentEtag = mostRecentAttachmentEtag,
                Stats = replicationTask == null ? new List<DestinationStats>() : replicationTask.DestinationStats.Values.ToList()
            };

            return replicationStatistics;
        }
    }
}
