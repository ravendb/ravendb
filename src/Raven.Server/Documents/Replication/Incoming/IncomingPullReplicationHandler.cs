using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
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
        private readonly bool _canOmitSourceItems;

        private AllowedPathsValidator _allowedPathsValidator;
        
        private readonly PullReplicationBatchHistory _hubBatchHistory;
        private readonly PullReplicationBatchHistory _sinkBatchHistory;

        public string CertificateThumbprint;

        public IncomingPullReplicationHandler(TcpConnectionOptions options, ReplicationLatestEtagRequest replicatedLastEtag, ReplicationLoader parent, JsonOperationContext.MemoryBuffer bufferToCopy, ReplicationLatestEtagRequest.ReplicationType replicationType, ReplicationLoader.PullReplicationParams pullReplicationParams) : 
            base(options, replicatedLastEtag, parent, bufferToCopy, replicationType)
        {
            Debug.Assert(pullReplicationParams != null);

            if (pullReplicationParams.AllowedPaths != null && pullReplicationParams.AllowedPaths.Length > 0)
                _allowedPathsValidator = new AllowedPathsValidator(pullReplicationParams.AllowedPaths);

            _incomingPullReplicationParams = new ReplicationLoader.PullReplicationParams
            {
                AllowedPaths = pullReplicationParams.AllowedPaths,
                Mode = pullReplicationParams.Mode,
                Name = pullReplicationParams.Name,
                SourceDatabaseName = replicatedLastEtag.SourceDatabaseName,
                PreventDeletionsMode = pullReplicationParams.PreventDeletionsMode,
                Type = ReplicationLoader.PullReplicationParams.ConnectionType.Incoming,
                TaskId = pullReplicationParams.TaskId
            };

            _preventIncomingSinkDeletions = _incomingPullReplicationParams.PreventDeletionsMode?.HasFlag(PreventDeletionsMode.PreventSinkToHubDeletions) == true &&
                                            _incomingPullReplicationParams.Mode == PullReplicationMode.SinkToHub;

            _canOmitSourceItems = replicatedLastEtag.CanOmitSourceItems || CanOmitSourceItems(_incomingPullReplicationParams);

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

        protected override DocumentMergedTransactionCommand GetMergeDocumentsCommand(DocumentsOperationContext context,
            DataForReplicationCommand data, long lastDocumentEtag)
        {
            var cmd = new MergedDocumentForPullReplicationCommand(data, lastDocumentEtag, _incomingPullReplicationParams, _canOmitSourceItems);
            foreach (var item in data.ReplicatedItems)
            {
                cmd.HandleExpiredDocuments(context, item);
            }

            return cmd;
        }

        protected override DocumentMergedTransactionCommand GetUpdateChangeVectorCommand(string changeVector, long lastDocumentEtag, IncomingConnectionInfo connectionInfo, AsyncManualResetEvent trigger)
        {
            return new MergedUpdateDatabaseChangeVectorForHubCommand(changeVector, lastDocumentEtag, ConnectionInfo, trigger, _incomingPullReplicationParams,
                _canOmitSourceItems);
        }

        protected override bool ShouldMergeHeartbeatChangeVector()
        {
            return ShouldMergePullHeartbeatChangeVector(_incomingPullReplicationParams.Mode, _canOmitSourceItems);
        }

        private static bool ShouldMergePullHeartbeatChangeVector(PullReplicationMode mode, bool canOmitSourceItems)
        {
            // A filtered boundary can skip source items, so the sender DB CV is not receiver DB coverage.
            if (canOmitSourceItems)
                return false;

            // Incoming pull params describe this TCP connection direction, not the raw pull definition flags.
            return mode switch
            {
                PullReplicationMode.HubToSink => true,
                PullReplicationMode.SinkToHub => false,
                PullReplicationMode.None => throw new InvalidOperationException("Incoming pull replication heartbeat cannot run with replication mode 'None'."),
                _ => throw new InvalidOperationException($"Incoming pull replication heartbeat cannot run with unexpected replication mode '{mode}'.")
            };
        }

        private static bool CanOmitSourceItems(ReplicationLoader.PullReplicationParams pullReplicationParams)
        {
            if (pullReplicationParams == null)
                return false;

            if (PullReplicationPathFilterUtils.CanOmitByAllowedPaths(pullReplicationParams.AllowedPaths))
                return true;

            return pullReplicationParams.Mode == PullReplicationMode.SinkToHub &&
                   pullReplicationParams.PreventDeletionsMode?.HasFlag(PreventDeletionsMode.PreventSinkToHubDeletions) == true;
        }

        internal sealed class MergedDocumentForPullReplicationCommand : MergedDocumentReplicationCommand
        {
            private readonly bool _isHub;
            private readonly bool _isSink;
            private readonly bool _canOmitSourceItems;
            private readonly PreventDeletionsMode? _preventDeletionsMode;

            public MergedDocumentForPullReplicationCommand(DataForReplicationCommand replicationInfo, long lastEtag,
                ReplicationLoader.PullReplicationParams pullReplicationParams, bool canOmitSourceItems) : base(replicationInfo, lastEtag)
            {
                _isHub = pullReplicationParams.Mode == PullReplicationMode.SinkToHub;
                _isSink = pullReplicationParams.Mode == PullReplicationMode.HubToSink;
                _canOmitSourceItems = canOmitSourceItems;
                _preventDeletionsMode = pullReplicationParams.PreventDeletionsMode;
            }

            protected override ChangeVector PreProcessItem(DocumentsOperationContext context, ReplicationBatchItem item)
            {
                if (_canOmitSourceItems == false && _isSink)
                    ReplaceKnownSinkEntries(context, ref item.ChangeVector);

                var changeVectorToMerge = item.ChangeVector;

                if (_canOmitSourceItems == false && _isHub)
                    changeVectorToMerge = ReplaceUnknownEntriesWithSinkTag(context, ref item.ChangeVector);

                if (_canOmitSourceItems == false)
                {
                    var parsedChangeVectorToMerge = context.GetChangeVector(changeVectorToMerge);
                    return parsedChangeVectorToMerge.IsSingle ? parsedChangeVectorToMerge : parsedChangeVectorToMerge.Order;
                }

                var filteredBoundaryChangeVectors = CreateFilteredBoundaryChangeVectors(context, item.ChangeVector);
                item.ChangeVector = filteredBoundaryChangeVectors.StoredItemChangeVector.AsString();
                return filteredBoundaryChangeVectors.DatabaseChangeVectorContribution;
            }

            private static (ChangeVector StoredItemChangeVector, ChangeVector DatabaseChangeVectorContribution) CreateFilteredBoundaryChangeVectors(
                DocumentsOperationContext context,
                string itemChangeVector)
            {
                var incomingChangeVector = context.GetChangeVector(itemChangeVector);
                var etag = context.DocumentDatabase.DocumentsStorage.GenerateNextEtag();
                var receiverLocalOrder = context.DocumentDatabase.DocumentsStorage.GetNewChangeVector(context, etag);
                var storedItemChangeVector = context.GetChangeVector(incomingChangeVector.Version.AsString(), receiverLocalOrder.AsString());

                return (storedItemChangeVector, receiverLocalOrder);
            }

            protected override string HandleRevisionTombstone(DocumentsOperationContext context, string changeVector)
            {
                if (_canOmitSourceItems == false)
                    ReplaceKnownSinkEntries(context, ref changeVector);

                return base.HandleRevisionTombstone(context, changeVector);
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

            internal static string ReplaceUnknownEntriesWithSinkTag(DocumentsOperationContext context, ref string changeVector)
            {
                var globalDbIds = context.LastDatabaseChangeVector?.AsString().ToChangeVectorList()?.Select(x => x.DbId).ToList();
                var parsedChangeVector = context.GetChangeVector(changeVector);
                var incomingVersion = parsedChangeVector.Version.AsString();
                var incoming = incomingVersion.ToChangeVectorList();
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

                var newVersion = newIncoming.SerializeVector();
                changeVector = parsedChangeVector.IsSingle
                    ? newVersion
                    : context.GetChangeVector(newVersion, parsedChangeVector.Order.AsString()).AsString();

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
        }

        internal sealed class MergedUpdateDatabaseChangeVectorForHubCommand : MergedUpdateDatabaseChangeVectorCommand
        {
            private readonly ReplicationLoader.PullReplicationParams _pullReplicationParams;
            private readonly bool _canOmitSourceItems;

            public MergedUpdateDatabaseChangeVectorForHubCommand(string changeVector, long lastDocumentEtag, IncomingConnectionInfo connectionInfo, AsyncManualResetEvent trigger,
                ReplicationLoader.PullReplicationParams pullReplicationParams, bool canOmitSourceItems) : base(changeVector, lastDocumentEtag, connectionInfo, trigger)
            {
                _pullReplicationParams = pullReplicationParams;
                _canOmitSourceItems = canOmitSourceItems;
            }
            protected override bool TryUpdateChangeVector(DocumentsOperationContext context)
            {
                if (ShouldMergePullHeartbeatChangeVector(_pullReplicationParams.Mode, _canOmitSourceItems) == false)
                    return false;

                return base.TryUpdateChangeVector(context);
            }

            public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, DocumentMergedTransactionCommand> ToDto(DocumentsOperationContext context)
            {
                return new MergedUpdateDatabaseChangeVectorForHubCommandDto
                {
                    BaseDto = (MergedUpdateDatabaseChangeVectorCommandDto)base.ToDto(context),
                    PullReplicationParams = _pullReplicationParams,
                    CanOmitSourceItems = _canOmitSourceItems
                };
            }
        }

        internal sealed class MergedUpdateDatabaseChangeVectorForHubCommandDto : IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedUpdateDatabaseChangeVectorForHubCommand>
        {
            public MergedUpdateDatabaseChangeVectorCommandDto BaseDto;
            public ReplicationLoader.PullReplicationParams PullReplicationParams;
            public bool CanOmitSourceItems;
            public MergedUpdateDatabaseChangeVectorForHubCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
            {
                var command = new MergedUpdateDatabaseChangeVectorForHubCommand(BaseDto.ChangeVector, BaseDto.LastDocumentEtag, BaseDto.IncomingConnectionInfo,
                    new AsyncManualResetEvent(), PullReplicationParams, CanOmitSourceItems);
                return command;
            }
        }
    }
}
