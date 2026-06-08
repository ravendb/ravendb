using System;
using System.Collections.Generic;
using Raven.Client;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Replication.Messages;
using Raven.Server.Documents.Replication.Outgoing;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Client.Extensions;
using Raven.Client.Util;
using Raven.Server.ServerWide.Commands;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;

namespace Raven.Server.Documents.Replication.Incoming
{
    public abstract partial class IncomingPullReplicationHandler : IncomingReplicationHandler
    {
        protected enum ChangeVectorShape
        {
            Flat,
            Composite
        }

        public readonly ReplicationLoader.PullReplicationParams IncomingPullReplicationParams;
        public readonly string CertificateThumbprint;

        private readonly PullReplicationBatchHistory _hubBatchHistory;
        private readonly PullReplicationBatchHistory _sinkBatchHistory;
        protected readonly ChangeVectorShape PullReplicationChangeVectorShape;

        private readonly AllowedPathsValidator _allowedPathsValidator;

        protected IncomingPullReplicationHandler(
            TcpConnectionOptions options,
            ReplicationLatestEtagRequest sourceHandshakeRequest,
            ReplicationLoader parent,
            JsonOperationContext.MemoryBuffer bufferToCopy,
            ReplicationLatestEtagRequest.ReplicationType replicationType,
            ReplicationLoader.PullReplicationParams incomingPullReplicationParams) : base(options, sourceHandshakeRequest, parent, bufferToCopy, replicationType)
        {
            if (incomingPullReplicationParams?.AllowedPaths != null && incomingPullReplicationParams.AllowedPaths.Length > 0)
                _allowedPathsValidator = new AllowedPathsValidator(incomingPullReplicationParams.AllowedPaths);

            IncomingPullReplicationParams = new ReplicationLoader.PullReplicationParams
            {
                AllowedPaths = incomingPullReplicationParams?.AllowedPaths,
                Mode = incomingPullReplicationParams?.Mode ?? PullReplicationMode.None,
                Name = incomingPullReplicationParams?.Name,
                SourceDatabaseName = sourceHandshakeRequest.SourceDatabaseName,
                PreventDeletionsMode = incomingPullReplicationParams?.PreventDeletionsMode,
                Type = ReplicationLoader.PullReplicationParams.ConnectionType.Incoming,
                TaskId = incomingPullReplicationParams?.TaskId ?? 0
            };

            // Sender-side filtering is declared in the handshake; receiver-side filtering is derived from this side's pull replication rules.
            var canFilterOutSourceItems = sourceHandshakeRequest.CanFilterOutSourceItems || CanReceiverFilterOutSourceItems(IncomingPullReplicationParams);
            var bothSidesSupportCompositeChangeVectors = sourceHandshakeRequest.SupportsPullReplicationCompositeChangeVectors &&
                                                               parent.Database.SupportedFeatures.SupportedFeatureTypes.PullReplicationCompositeChangeVectors;
            PullReplicationChangeVectorShape = canFilterOutSourceItems && bothSidesSupportCompositeChangeVectors
                ? ChangeVectorShape.Composite
                : ChangeVectorShape.Flat;

            CertificateThumbprint = options.Certificate?.Thumbprint;

            AfterItemsReadFromStream = ValidateIncomingReplicationItemsPaths;

            _hubBatchHistory = new PullReplicationBatchHistory(parent);
            _sinkBatchHistory = new PullReplicationBatchHistory(parent);
        }

        public override string FromToString => base.FromToString +
                                               $"{(IncomingPullReplicationParams?.Name == null ? null : $"(pull definition: {IncomingPullReplicationParams?.Name})")}";

        protected override void HandleHeartbeatMessage(DocumentsOperationContext documentsContext, BlittableJsonReaderObject message)
        {
            if (PullReplicationChangeVectorShape == ChangeVectorShape.Composite)
                return;

            HandleFlatHeartbeatMessage(documentsContext, message);
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

        protected abstract bool PreventIncomingSinkDeletions { get; }

        protected abstract string GetChangeVectorForHeartbeatUpdate(DocumentsOperationContext context, string changeVector);

        protected abstract DocumentMergedTransactionCommand CreateHeartbeatUpdateCommand(string changeVector);

        private void ValidateIncomingReplicationItemsPaths(DataForReplicationCommand dataForReplicationCommand)
        {
            if (_allowedPathsValidator == null && PreventIncomingSinkDeletions == false)
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

                if (PreventIncomingSinkDeletions)
                {
                    if (item.IsPreventableSinkToHubDeletion())
                    {
                        using (var infoHelper = new DocumentInfoHelper())
                        {
                            throw new InvalidOperationException(
                                $"This hub does not allow for tombstone replication via pull replication '{IncomingPullReplicationParams.Name}'." +
                                $" Replication of item '{infoHelper.GetItemInformation(item)}' has been aborted for sink connection: '{ConnectionInfo}'.");
                        }
                    }
                }
            }
        }

        protected override DynamicJsonValue GetHeartbeatStatusMessage(DocumentsOperationContext documentsContext, long lastDocumentEtag, string handledMessageType)
        {
            var heartbeat = base.GetHeartbeatStatusMessage(documentsContext, lastDocumentEtag, handledMessageType);

            if (IncomingPullReplicationParams.Mode == PullReplicationMode.SinkToHub)
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
            else if (IncomingPullReplicationParams.Mode == PullReplicationMode.HubToSink)
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
                    TaskId = IncomingPullReplicationParams.TaskId,
                    NodeTag = ReplicationLoaderParent._server.NodeTag,
                    SourceChangeVector = confirmedHubCv,
                    Type = ExternalReplicationState.ReplicationStateType.HubCursor
                }
            };
            ReplicationLoaderParent._server.SendToLeaderAsync(command).IgnoreUnobservedExceptions();
        }

        private void HandleFlatHeartbeatMessage(DocumentsOperationContext documentsContext, BlittableJsonReaderObject message)
        {
            if (message.TryGet(nameof(ReplicationMessageHeader.DatabaseChangeVector), out string changeVector) == false)
                return;

            long lastEtag;
            string lastChangeVector;
            using (documentsContext.OpenReadTransaction())
            {
                lastEtag = DocumentsStorage.GetLastReplicatedEtagFrom(documentsContext, ConnectionInfo.SourceDatabaseId);
                lastChangeVector = DocumentsStorage.GetDatabaseChangeVector(documentsContext);
            }

            changeVector = GetChangeVectorForHeartbeatUpdate(documentsContext, changeVector);

            var status = ChangeVectorUtils.GetConflictStatus(changeVector, lastChangeVector);
            if (status != ConflictStatus.Update && _lastDocumentEtag <= lastEtag)
                return;

            if (Logger.IsDebugEnabled)
            {
                Logger.Debug(
                    $"Try to update the current database change vector ({lastChangeVector}) with {changeVector} in status {status}" +
                    $"with etag: {_lastDocumentEtag} (new) > {lastEtag} (old)");
            }

            var cmd = CreateHeartbeatUpdateCommand(changeVector);
            EnqueueUpdateChangeVectorCommand(cmd);
        }

        private bool CanReceiverFilterOutSourceItems(ReplicationLoader.PullReplicationParams pullReplicationParams)
        {
            if (pullReplicationParams == null)
                return false;

            if (PullReplicationPathFilterUtils.CanFilterOutByAllowedPaths(pullReplicationParams.AllowedPaths))
                return true;

            return PreventIncomingSinkDeletions;
        }

        protected static void RemoveExpiresFromSinkBatchItem(DocumentsOperationContext ctx, ReplicationBatchItem item, bool preventIncomingSinkDeletions)
        {
            if (preventIncomingSinkDeletions == false)
                return;

            if (item is not DocumentReplicationItem doc)
                return;

            if (doc.Data == null)
                return;

            if (doc.Data.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) == false)
                return;

            if (metadata.TryGet(Constants.Documents.Metadata.Expires, out string _) == false)
                return;

            metadata.Modifications ??= new DynamicJsonValue(metadata);
            metadata.Modifications.Remove(Constants.Documents.Metadata.Expires);
            using (doc.Data)
            {
                doc.Data = ctx.ReadObject(doc.Data, doc.Id, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
            }
        }
    }
}
