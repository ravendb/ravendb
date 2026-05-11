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
                SourceDatabaseName = replicatedLastEtag.SourceDatabaseName,
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

        protected override bool ShouldMergeHeartbeatChangeVector() => ShouldMergePullHeartbeatChangeVector(_incomingPullReplicationParams.Mode);

        private static bool ShouldMergePullHeartbeatChangeVector(PullReplicationMode mode)
        {
            // Incoming pull params describe this TCP connection direction, not the raw pull definition flags.
            return mode switch
            {
                PullReplicationMode.HubToSink => true,
                PullReplicationMode.SinkToHub => false,
                PullReplicationMode.None => throw new InvalidOperationException("Incoming pull replication heartbeat cannot run with replication mode 'None'."),
                _ => throw new InvalidOperationException($"Incoming pull replication heartbeat cannot run with unexpected replication mode '{mode}'.")
            };
        }

        internal sealed class MergedDocumentForPullReplicationCommand : MergedDocumentReplicationCommand
        {
            private readonly bool _isSink;
            private readonly PreventDeletionsMode? _preventDeletionsMode;

            public MergedDocumentForPullReplicationCommand(DataForReplicationCommand replicationInfo, long lastEtag,
                ReplicationLoader.PullReplicationParams pullReplicationParams) : base(replicationInfo, lastEtag)
            {
                _isSink = pullReplicationParams.Mode == PullReplicationMode.HubToSink;
                _preventDeletionsMode = pullReplicationParams.PreventDeletionsMode;
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
                if (ShouldMergePullHeartbeatChangeVector(_pullReplicationParams.Mode) == false)
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
