using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Replication.Messages;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.Replication.Stats;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server;
using Voron;

namespace Raven.Server.Documents.Replication.Incoming
{
    public sealed class IncomingPullReplicationHandler : IncomingReplicationHandler
    {
        public readonly ReplicationLoader.PullReplicationParams _incomingPullReplicationParams;

        private readonly bool _preventIncomingSinkDeletions;

        private AllowedPathsValidator _allowedPathsValidator;

        public string CertificateThumbprint;
        public IncomingPullReplicationHandler(TcpConnectionOptions options, ReplicationLatestEtagRequest replicatedLastEtag, ReplicationLoader parent, JsonOperationContext.MemoryBuffer bufferToCopy, ReplicationLatestEtagRequest.ReplicationType replicationType, ReplicationLoader.PullReplicationParams pullReplicationParams) : 
            base(options, replicatedLastEtag, parent, bufferToCopy, replicationType)
        {
            if (pullReplicationParams?.AllowedPaths != null && pullReplicationParams.AllowedPaths.Length > 0)
                _allowedPathsValidator = new AllowedPathsValidator(pullReplicationParams.AllowedPaths);

            _incomingPullReplicationParams = new ReplicationLoader.PullReplicationParams
            {
                AllowedPaths = pullReplicationParams?.AllowedPaths,
                Mode = pullReplicationParams?.Mode ?? PullReplicationMode.None,
                Name = pullReplicationParams?.Name,
                PreventDeletionsMode = pullReplicationParams?.PreventDeletionsMode,
                Type = ReplicationLoader.PullReplicationParams.ConnectionType.Incoming
            };

            _preventIncomingSinkDeletions = _incomingPullReplicationParams.PreventDeletionsMode?.HasFlag(PreventDeletionsMode.PreventSinkToHubDeletions) == true &&
                                            _incomingPullReplicationParams.Mode == PullReplicationMode.SinkToHub;


            CertificateThumbprint = options.Certificate?.Thumbprint;

            AfterItemsReadFromStream = ValidateIncomingReplicationItemsPaths;
        }

        private void ValidateIncomingReplicationItemsPaths(DataForReplicationCommand dataForReplicationCommand)
        {
            if (_allowedPathsValidator == null && _preventIncomingSinkDeletions == false)
                return;

            HashSet<Slice> expectedAttachmentStreams = null;

            foreach (var item in dataForReplicationCommand.ReplicatedItems)
            {
                if (_allowedPathsValidator != null)
                {
                    if (_allowedPathsValidator.ShouldAllow(item) == false)
                    {
                        throw new InvalidOperationException("Attempted to replicate " + _allowedPathsValidator.GetItemInformation(item) +
                                                            ", which is not allowed, according to the allowed paths policy. Replication aborted");
                    }

                    switch (item)
                    {
                        case AttachmentReplicationItem a:
                            expectedAttachmentStreams ??= new HashSet<Slice>(SliceComparer.Instance);
                            expectedAttachmentStreams.Add(a.Key);
                            break;
                    }
                }

                if (_preventIncomingSinkDeletions)
                {
                    if (ReplicationLoader.IsOfTypePreventDeletions(item))
                    {
                        using (var infoHelper = new DocumentInfoHelper())
                        {
                            throw new InvalidOperationException(
                                $"This hub does not allow for tombstone replication via pull replication '{_incomingPullReplicationParams.Name}'." +
                                $" Replication of item '{infoHelper.GetItemInformation(item)}' has been aborted for sink connection: '{this.ConnectionInfo.ToString()}'.");
                        }
                    }
                }
            }
        }

        protected override void DisposeInternal()
        {
            try
            {
                _allowedPathsValidator?.Dispose();
            }
            catch
            {
                // ignore
            }
            base.DisposeInternal();
        }

        public override string FromToString => base.FromToString +
                                               $"{(_incomingPullReplicationParams?.Name == null ? null : $"(pull definition: {_incomingPullReplicationParams?.Name})")}";

        protected override DocumentMergedTransactionCommand GetMergeDocumentsCommand(DocumentsOperationContext context,
            DataForReplicationCommand data, long lastDocumentEtag)
        {
            var cmd = new MergedDocumentForPullReplicationCommand(data, lastDocumentEtag, _incomingPullReplicationParams);
            foreach (var item in data.ReplicatedItems)
            {
                cmd.HandleExpiredDocuments(context, item);
            }

            return cmd;
        }

        protected override DocumentMergedTransactionCommand GetUpdateChangeVectorCommand(string changeVector, long lastDocumentEtag, IncomingConnectionInfo connectionInfo, AsyncManualResetEvent trigger)
        {
            return new MergedUpdateDatabaseChangeVectorForHubCommand(changeVector, lastDocumentEtag, ConnectionInfo, trigger, _incomingPullReplicationParams);
        }

        internal sealed class MergedDocumentForPullReplicationCommand : MergedDocumentReplicationCommand
        {
            private readonly bool _isHub;
            private readonly bool _isSink;
            private readonly PreventDeletionsMode? _preventDeletionsMode;

            public MergedDocumentForPullReplicationCommand(DataForReplicationCommand replicationInfo, long lastEtag,
                ReplicationLoader.PullReplicationParams pullReplicationParams) : base(replicationInfo, lastEtag)
            {
                _isHub = pullReplicationParams.Mode == PullReplicationMode.SinkToHub;
                _isSink = pullReplicationParams.Mode == PullReplicationMode.HubToSink;
                _preventDeletionsMode = pullReplicationParams.PreventDeletionsMode;
            }

            protected override ChangeVector PreProcessItem(DocumentsOperationContext context, ReplicationBatchItem item)
            {
                if (_isSink) 
                    ReplaceKnownSinkEntries(context, ref item.ChangeVector);

                var changeVectorToMerge = item.ChangeVector;

                if (_isHub) 
                    changeVectorToMerge = ReplaceUnknownEntriesWithSinkTag(context, ref item.ChangeVector);

                return context.GetChangeVector(changeVectorToMerge);
            }

            protected override void HandleRevisionTombstone(DocumentsOperationContext context, string docId, string changeVector, out Slice changeVectorSlice, out Slice keySlice, List<IDisposable> toDispose)
            {
                ReplaceKnownSinkEntries(context, ref changeVector);
                base.HandleRevisionTombstone(context, docId, changeVector, out changeVectorSlice, out keySlice, toDispose);
            }

            public void HandleExpiredDocuments(DocumentsOperationContext ctx, ReplicationBatchItem item)
            {
                if (_isSink)
                    return;

                if (_preventDeletionsMode?.HasFlag(PreventDeletionsMode.PreventSinkToHubDeletions) == false)
                    return;

                if (item is DocumentReplicationItem doc)
                {
                    if (doc.Data == null)
                        return;

                    RemoveExpiresFromSinkBatchItem(doc, ctx);
                }
            }

            private static string ReplaceUnknownEntriesWithSinkTag(DocumentsOperationContext context, ref string changeVector)
            {
                var globalDbIds = context.LastDatabaseChangeVector?.AsString().ToChangeVectorList()?.Select(x => x.DbId).ToList();
                var incoming = changeVector.ToChangeVectorList();
                var knownEntries = new List<ChangeVectorEntry>();
                var newIncoming = new List<ChangeVectorEntry>();

                foreach (var entry in incoming)
                {
                    if (globalDbIds?.Contains(entry.DbId) == true)
                    {
                        newIncoming.Add(entry);
                        knownEntries.Add(entry);
                    }
                    else if (entry.DbId == context.DocumentDatabase.ClusterTransactionId)
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
                    else
                    {
                        newIncoming.Add(new ChangeVectorEntry
                        {
                            DbId = entry.DbId,
                            Etag = entry.Etag,
                            NodeTag = ChangeVectorParser.SinkInt
                        });

                        context.DbIdsToIgnore ??= new HashSet<string>();
                        context.DbIdsToIgnore.Add(entry.DbId);
                    }
                }

                changeVector = newIncoming.SerializeVector();

                return knownEntries.Count > 0 ? 
                    knownEntries.SerializeVector() : 
                    null;
            }

            private static void ReplaceKnownSinkEntries(DocumentsOperationContext context, ref string changeVector)
            {
                if (changeVector.Contains(ChangeVectorParser.SinkTag, StringComparison.OrdinalIgnoreCase) == false)
                    return;

                var global = context.LastDatabaseChangeVector?.AsString().ToChangeVectorList();
                var incoming = changeVector.ToChangeVectorList();
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

                changeVector = newIncoming.SerializeVector();
            }

            private static void RemoveExpiresFromSinkBatchItem(DocumentReplicationItem doc, JsonOperationContext context)
            {
                if (doc.Data.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) == false)
                    return;

                if (metadata.TryGet(Constants.Documents.Metadata.Expires, out string _) == false)
                    return;

                metadata.Modifications ??= new DynamicJsonValue(metadata);
                metadata.Modifications.Remove(Constants.Documents.Metadata.Expires);
                using (var old = doc.Data)
                {
                    doc.Data = context.ReadObject(doc.Data, doc.Id, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                }
            }
        }

        internal sealed class MergedUpdateDatabaseChangeVectorForHubCommand : MergedUpdateDatabaseChangeVectorCommand
        {
            private readonly ReplicationLoader.PullReplicationParams _pullReplicationParams;

            public MergedUpdateDatabaseChangeVectorForHubCommand(string changeVector, long lastDocumentEtag, IncomingConnectionInfo connectionInfo, AsyncManualResetEvent trigger,
                ReplicationLoader.PullReplicationParams pullReplicationParams) : base(changeVector, lastDocumentEtag, connectionInfo, trigger)
            {
                _pullReplicationParams = pullReplicationParams;
            }
            protected override bool TryUpdateChangeVector(DocumentsOperationContext context)
            {
                if (_pullReplicationParams.Mode == PullReplicationMode.SinkToHub)
                    return false;

                return base.TryUpdateChangeVector(context);
            }

            public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, DocumentMergedTransactionCommand> ToDto(DocumentsOperationContext context)
            {
                return new MergedUpdateDatabaseChangeVectorForHubCommandDto
                {
                    BaseDto = (MergedUpdateDatabaseChangeVectorCommandDto)base.ToDto(context),
                    PullReplicationParams = _pullReplicationParams
                };
            }
        }

        internal sealed class MergedUpdateDatabaseChangeVectorForHubCommandDto : IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedUpdateDatabaseChangeVectorForHubCommand>
        {
            public MergedUpdateDatabaseChangeVectorCommandDto BaseDto;
            public ReplicationLoader.PullReplicationParams PullReplicationParams;
            public MergedUpdateDatabaseChangeVectorForHubCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
            {
                var command = new MergedUpdateDatabaseChangeVectorForHubCommand(BaseDto.ChangeVector, BaseDto.LastDocumentEtag, BaseDto.IncomingConnectionInfo,
                    new AsyncManualResetEvent(), PullReplicationParams);
                return command;
            }
        }
    }

}
