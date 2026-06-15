using System;
using System.Collections.Generic;
using Raven.Client;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Replication.Messages;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;

namespace Raven.Server.Documents.Replication.Incoming
{
    public abstract partial class IncomingPullReplicationHandler : IncomingReplicationHandler
    {
        public readonly ReplicationLoader.PullReplicationParams IncomingPullReplicationParams;
        public readonly string CertificateThumbprint;

        protected readonly PullReplicationChangeVectorShape ChangeVectorShape;

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
            var changeVectorTransmission = PullReplicationChangeVectorModeSelector.GetChangeVectorTransmission(
                localSupportsCompositeChangeVectors: parent.Database.SupportedFeatures.SupportedFeatureTypes.PullReplicationCompositeChangeVectors,
                remoteSupportsCompositeChangeVectors: sourceHandshakeRequest.SupportsPullReplicationCompositeChangeVectors);

            ChangeVectorShape = PullReplicationChangeVectorModeSelector.GetChangeVectorShape(
                canFilterOutSourceItems: sourceHandshakeRequest.CanFilterOutSourceItems || CanReceiverFilterOutSourceItems(IncomingPullReplicationParams),
                transmission: changeVectorTransmission);

            CertificateThumbprint = options.Certificate?.Thumbprint;

            AfterItemsReadFromStream = ValidateIncomingReplicationItemsPaths;
        }

        public override string FromToString => base.FromToString +
                                               $"{(IncomingPullReplicationParams?.Name == null ? null : $"(pull definition: {IncomingPullReplicationParams?.Name})")}";

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
