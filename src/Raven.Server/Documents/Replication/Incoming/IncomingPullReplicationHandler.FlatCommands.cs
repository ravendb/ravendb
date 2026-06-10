using System;
using System.Collections.Generic;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;

namespace Raven.Server.Documents.Replication.Incoming
{
    public abstract partial class IncomingPullReplicationHandler
    {
        internal abstract class MergedFlatPullReplicationCommand : MergedDocumentReplicationCommand
        {
            protected MergedFlatPullReplicationCommand(DataForReplicationCommand replicationInfo, long lastEtag) : base(replicationInfo, lastEtag)
            {
            }

            protected override string HandleRevisionTombstone(DocumentsOperationContext context, string changeVector)
            {
                RestoreKnownSinkEntriesFromLocalChangeVector(context, ref changeVector);
                return base.HandleRevisionTombstone(context, changeVector);
            }

        }

        internal sealed class MergedFlatPullReplicationOnHubCommand : MergedFlatPullReplicationCommand
        {
            private readonly bool _preventIncomingSinkDeletions;

            public MergedFlatPullReplicationOnHubCommand(DataForReplicationCommand replicationInfo, long lastEtag, bool preventIncomingSinkDeletions) : base(replicationInfo, lastEtag)
            {
                _preventIncomingSinkDeletions = preventIncomingSinkDeletions;
            }

            protected override ChangeVector PreProcessItem(DocumentsOperationContext context, ReplicationBatchItem item)
            {
                RemoveExpiresFromSinkBatchItem(context, item, _preventIncomingSinkDeletions);

                var changeVectorToMerge = MaskUnknownVersionEntriesWithSinkTag(context, ref item.ChangeVector);
                var parsedChangeVectorToMerge = context.GetChangeVector(changeVectorToMerge);
                return parsedChangeVectorToMerge.IsSingle ? parsedChangeVectorToMerge : parsedChangeVectorToMerge.Order;
            }
        }

        internal sealed class MergedFlatPullReplicationOnSinkCommand : MergedFlatPullReplicationCommand
        {
            public MergedFlatPullReplicationOnSinkCommand(DataForReplicationCommand replicationInfo, long lastEtag) : base(replicationInfo, lastEtag)
            {
            }

            protected override ChangeVector PreProcessItem(DocumentsOperationContext context, ReplicationBatchItem item)
            {
                RestoreKnownSinkEntriesFromLocalChangeVector(context, ref item.ChangeVector);

                var parsedChangeVectorToMerge = context.GetChangeVector(item.ChangeVector);
                return parsedChangeVectorToMerge.IsSingle ? parsedChangeVectorToMerge : parsedChangeVectorToMerge.Order;
            }
        }

        protected static string MaskUnknownVersionEntriesWithSinkTag(DocumentsOperationContext context, ref string changeVector)
        {
            if (string.IsNullOrEmpty(changeVector))
                return string.Empty;

            var parsedChangeVector = context.GetChangeVector(changeVector);
            var knownEntries = new List<ChangeVectorEntry>();
            var newVersion = ChangeVectorUtils.MaskUnknownEntriesWithSinkTag(context, parsedChangeVector.Version, context.LastDatabaseChangeVector, knownEntries, trackIgnoredDbIds: true);
            changeVector = parsedChangeVector.IsSingle
                ? newVersion
                : context.GetChangeVector(newVersion, parsedChangeVector.Order);

            return knownEntries.Count > 0 ?
                knownEntries.SerializeVector() :
                null;
        }

        protected static void RestoreKnownSinkEntriesFromLocalChangeVector(DocumentsOperationContext context, ref string changeVector)
        {
            var parsedChangeVector = context.GetChangeVector(changeVector);
            var incomingVersion = parsedChangeVector.Version.AsString();

            if (incomingVersion.Contains(ChangeVectorParser.SinkTag, StringComparison.OrdinalIgnoreCase) == false)
                return;

            var global = context.LastDatabaseChangeVector?.AsString().ToChangeVectorList();
            var incoming = incomingVersion.ToChangeVectorList();
            var newIncoming = new List<ChangeVectorEntry>();

            foreach (var entry in incoming)
            {
                if (entry.NodeTag == ChangeVectorParser.SinkInt)
                {
                    var found = global?.Find(x => x.DbId == entry.DbId) ?? default;
                    if (found.Etag > 0)
                    {
                        newIncoming.Add(new ChangeVectorEntry
                        {
                            DbId = entry.DbId,
                            Etag = entry.Etag,
                            NodeTag = found.NodeTag
                        });
                        continue;
                    }
                }

                if (entry.DbId == context.DocumentDatabase.ClusterTransactionId)
                {
                    // TRXN
                    newIncoming.Add(new ChangeVectorEntry
                    {
                        DbId = entry.DbId,
                        Etag = entry.Etag,
                        NodeTag = ChangeVectorParser.TrxnInt
                    });

                    continue;
                }

                newIncoming.Add(entry);
            }

            var newVersion = newIncoming.SerializeVector();
            changeVector = parsedChangeVector.IsSingle
                ? newVersion
                : context.GetChangeVector(newVersion, parsedChangeVector.Order.AsString()).AsString();
        }
    }
}
