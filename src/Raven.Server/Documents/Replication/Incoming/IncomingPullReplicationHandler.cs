using System;
using System.Collections.Generic;
using Raven.Client;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Replication.Messages;
using Raven.Server.Documents.Replication.Outgoing;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.Replication.Stats;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Client.Extensions;
using Raven.Client.Util;
using Raven.Server.ServerWide.Commands;
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
        private readonly bool _useCompositeChangeVectors;

        private AllowedPathsValidator _allowedPathsValidator;
        
        private readonly PullReplicationBatchHistory _hubBatchHistory;
        private readonly PullReplicationBatchHistory _sinkBatchHistory;

        public string CertificateThumbprint;

        public IncomingPullReplicationHandler(
            TcpConnectionOptions options,
            ReplicationLatestEtagRequest sourceHandshakeRequest,
            ReplicationLoader parent,
            JsonOperationContext.MemoryBuffer bufferToCopy,
            ReplicationLatestEtagRequest.ReplicationType replicationType,
            ReplicationLoader.PullReplicationParams incomingPullReplicationParams) : base(options, sourceHandshakeRequest, parent, bufferToCopy, replicationType)
        {
            if (incomingPullReplicationParams?.AllowedPaths != null && incomingPullReplicationParams.AllowedPaths.Length > 0)
                _allowedPathsValidator = new AllowedPathsValidator(incomingPullReplicationParams.AllowedPaths);

            _incomingPullReplicationParams = new ReplicationLoader.PullReplicationParams
            {
                AllowedPaths = incomingPullReplicationParams?.AllowedPaths,
                Mode = incomingPullReplicationParams?.Mode ?? PullReplicationMode.None,
                Name = incomingPullReplicationParams?.Name,
                SourceDatabaseName = sourceHandshakeRequest.SourceDatabaseName,
                PreventDeletionsMode = incomingPullReplicationParams?.PreventDeletionsMode,
                Type = ReplicationLoader.PullReplicationParams.ConnectionType.Incoming,
                TaskId = incomingPullReplicationParams?.TaskId ?? 0
            };

            _preventIncomingSinkDeletions = _incomingPullReplicationParams.PreventDeletionsMode?.HasFlag(PreventDeletionsMode.PreventSinkToHubDeletions) == true &&
                                            _incomingPullReplicationParams.Mode == PullReplicationMode.SinkToHub;

            // Sender-side filtering is declared in the handshake; receiver-side filtering is derived from this side's pull replication rules.
            var canFilterOutSourceItems = sourceHandshakeRequest.CanFilterOutSourceItems || CanReceiverFilterOutSourceItems(_incomingPullReplicationParams);
            var sourceSupportsCompositeChangeVectors = sourceHandshakeRequest.SupportsPullReplicationCompositeChangeVectors;
            var receiverSupportsCompositeChangeVectors = parent.Database.SupportedFeatures.SupportedFeatureTypes.PullReplicationCompositeChangeVectors;
            var bothSidesSupportCompositeChangeVectors = sourceSupportsCompositeChangeVectors && receiverSupportsCompositeChangeVectors;
            _useCompositeChangeVectors = canFilterOutSourceItems && bothSidesSupportCompositeChangeVectors;

            CertificateThumbprint = options.Certificate?.Thumbprint;

            AfterItemsReadFromStream = ValidateIncomingReplicationItemsPaths;

            _hubBatchHistory = new PullReplicationBatchHistory(parent);
            _sinkBatchHistory = new PullReplicationBatchHistory(parent);
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

        protected override DynamicJsonValue GetHeartbeatStatusMessage(DocumentsOperationContext documentsContext, long lastDocumentEtag, string handledMessageType)
        {
            var heartbeat = base.GetHeartbeatStatusMessage(documentsContext, lastDocumentEtag, handledMessageType);

            if (_incomingPullReplicationParams.Mode == PullReplicationMode.SinkToHub)
            {
                switch (handledMessageType)
                {
                    case ReplicationMessageType.Documents:
                    case ReplicationMessageType.Heartbeat:
                        if (string.IsNullOrEmpty(_lastBatchChangeVector) == false)
                        {
                            long hubEtag = (long)heartbeat[nameof(ReplicationMessageReply.CurrentEtag)];
                            _sinkBatchHistory.Add(hubEtag, _lastBatchChangeVector);
                            
                        }
                        break;
                }

                // Here we report to the sink about the last sink change vector that was replicated to all the nodes in the hub cluster
                heartbeat[nameof(ReplicationMessageReply.LastConfirmedChangeVector)] = _sinkBatchHistory.ComputeConfirmedChangeVector(_lastBatchChangeVector);
            }
            else if (_incomingPullReplicationParams.Mode == PullReplicationMode.HubToSink)
            {
                switch (handledMessageType)
                {
                    case ReplicationMessageType.Documents:
                    case ReplicationMessageType.Heartbeat:
                        if (string.IsNullOrEmpty(_lastBatchChangeVector) == false)
                        {
                            long sinkEtag = (long)heartbeat[nameof(ReplicationMessageReply.CurrentEtag)];
                            _hubBatchHistory.Add(sinkEtag, _lastBatchChangeVector);
                        }
                        break;
                }

                // Here we check *locally* in the sink what is the last hub change vector that was replicated to all the nodes in the sink cluster
                if (_hubBatchHistory.ComputeConfirmedChangeVector(_lastBatchChangeVector) is { } confirmedHubCv)
                    PersistHubCursor(confirmedHubCv);
            }

            return heartbeat;
        }

        private void PersistHubCursor(string confirmedHubCv)
        {
            var command = new UpdateExternalReplicationStateCommand(ReplicationLoaderParent.Database.Name, RaftIdGenerator.NewId())
            {
                ExternalReplicationState = new ExternalReplicationState
                {
                    TaskId = _incomingPullReplicationParams.TaskId,
                    NodeTag = ReplicationLoaderParent._server.NodeTag,
                    SourceChangeVector = confirmedHubCv,
                    Type = ExternalReplicationState.ReplicationStateType.HubCursor
                }
            };
            ReplicationLoaderParent._server.SendToLeaderAsync(command).IgnoreUnobservedExceptions();
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

        protected override string ReplaceUnknownEntriesWithSinkIfNeeded(DocumentsOperationContext context, string changeVector)
        {
            if (_useCompositeChangeVectors)
                return changeVector;

            var isHub = _incomingPullReplicationParams.Mode == PullReplicationMode.SinkToHub;
            if (isHub && string.IsNullOrEmpty(changeVector) == false)
                changeVector = ReplaceUnknownEntriesWithSinkTag(context, ref changeVector);

            return changeVector;
        }

        protected override DocumentMergedTransactionCommand GetMergeDocumentsCommand(DocumentsOperationContext context,
            DataForReplicationCommand data, long lastDocumentEtag)
        {
            foreach (var item in data.ReplicatedItems)
            {
                HandleExpiredDocuments(context, item, _incomingPullReplicationParams);
            }

            if (_useCompositeChangeVectors)
                return new MergedFilteredPullReplicationCommand(data, lastDocumentEtag);

            return new MergedLegacyPullReplicationCommand(data, lastDocumentEtag, _incomingPullReplicationParams);
        }

        private static void HandleExpiredDocuments(DocumentsOperationContext ctx, ReplicationBatchItem item, ReplicationLoader.PullReplicationParams pullReplicationParams)
        {
            var isSink = pullReplicationParams.Mode == PullReplicationMode.HubToSink;
            if (isSink)
                return;

            if (pullReplicationParams.PreventDeletionsMode?.HasFlag(PreventDeletionsMode.PreventSinkToHubDeletions) == false)
                return;

            if (item is DocumentReplicationItem doc)
            {
                if (doc.Data == null)
                    return;

                RemoveExpiresFromSinkBatchItem(doc, ctx);
            }
        }

        protected override DocumentMergedTransactionCommand GetUpdateChangeVectorCommand(string changeVector, long lastDocumentEtag, IncomingConnectionInfo connectionInfo, AsyncManualResetEvent trigger)
        {
            return new MergedUpdateDatabaseChangeVectorForHubCommand(changeVector, lastDocumentEtag, ConnectionInfo, trigger, _incomingPullReplicationParams);
        }

        protected override void HandleHeartbeatMessage(DocumentsOperationContext documentsContext, BlittableJsonReaderObject message)
        {
            if (_useCompositeChangeVectors)
                return;

            base.HandleHeartbeatMessage(documentsContext, message);
        }

        private bool CanReceiverFilterOutSourceItems(ReplicationLoader.PullReplicationParams pullReplicationParams)
        {
            if (pullReplicationParams == null)
                return false;

            if (PullReplicationPathFilterUtils.CanFilterOutByAllowedPaths(pullReplicationParams.AllowedPaths))
                return true;

            return _preventIncomingSinkDeletions;
        }

        private sealed class MergedLegacyPullReplicationCommand : MergedDocumentReplicationCommand
        {
            private readonly bool _isHub;
            private readonly bool _isSink;

            public MergedLegacyPullReplicationCommand(DataForReplicationCommand replicationInfo, long lastEtag, ReplicationLoader.PullReplicationParams pullReplicationParams) : base(replicationInfo, lastEtag)
            {
                _isHub = pullReplicationParams.Mode == PullReplicationMode.SinkToHub;
                _isSink = pullReplicationParams.Mode == PullReplicationMode.HubToSink;
            }

            protected override ChangeVector PreProcessItem(DocumentsOperationContext context, ReplicationBatchItem item)
            {
                if (_isSink)
                    ReplaceKnownSinkEntries(context, ref item.ChangeVector);

                var changeVectorToMerge = item.ChangeVector;

                if (_isHub)
                    changeVectorToMerge = ReplaceUnknownEntriesWithSinkTag(context, ref item.ChangeVector);

                var parsedChangeVectorToMerge = context.GetChangeVector(changeVectorToMerge);
                return parsedChangeVectorToMerge.IsSingle ? parsedChangeVectorToMerge : parsedChangeVectorToMerge.Order;
            }

            protected override void HandleRevisionTombstone(DocumentsOperationContext context, string docId, string changeVector, out Slice changeVectorSlice, out Slice keySlice, List<IDisposable> toDispose)
            {
                ReplaceKnownSinkEntries(context, ref changeVector);
                base.HandleRevisionTombstone(context, docId, changeVector, out changeVectorSlice, out keySlice, toDispose);
            }
        }

        private sealed class MergedFilteredPullReplicationCommand : MergedDocumentReplicationCommand
        {
            public MergedFilteredPullReplicationCommand(DataForReplicationCommand replicationInfo, long lastEtag) : base(replicationInfo, lastEtag)
            {
            }

            protected override ChangeVector PreProcessItem(DocumentsOperationContext context, ReplicationBatchItem item)
            {
                var incomingChangeVector = context.GetChangeVector(item.ChangeVector);
                var etag = context.DocumentDatabase.DocumentsStorage.GenerateNextEtag();
                var receiverLocalOrder = context.DocumentDatabase.DocumentsStorage.GetNewChangeVector(context, etag);

                // Store receiver-local order with the incoming version lineage.
                item.ChangeVector = context.GetChangeVector(incomingChangeVector.Version, receiverLocalOrder).AsString();
                return receiverLocalOrder;
            }
        }

        private static string ReplaceUnknownEntriesWithSinkTag(DocumentsOperationContext context, ref string changeVector)
        {
            var parsedChangeVector = context.GetChangeVector(changeVector);
            var knownEntries = new List<ChangeVectorEntry>();
            var newVersion = ChangeVectorUtils.ReplaceUnknownEntriesWithSinkTag(context, parsedChangeVector.Version, context.LastDatabaseChangeVector, knownEntries, trackIgnoredDbIds: true);
            changeVector = parsedChangeVector.IsSingle
                ? newVersion
                : context.GetChangeVector(newVersion, parsedChangeVector.Order);

            return knownEntries.Count > 0 ?
                knownEntries.SerializeVector() :
                null;
        }

        private static void ReplaceKnownSinkEntries(DocumentsOperationContext context, ref string changeVector)
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
