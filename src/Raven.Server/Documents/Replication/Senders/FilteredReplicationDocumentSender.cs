using System.IO;
using Raven.Server.Documents.Replication.Outgoing;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.Replication.Stats;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Server.Logging;

namespace Raven.Server.Documents.Replication.Senders
{
    public sealed class FilteredReplicationDocumentSender : ExternalReplicationDocumentSender
    {
        private readonly AllowedPathsValidator _pathsToSend, _destinationAcceptablePaths;
        private readonly bool _shouldSkipSendingTombstones;
        private readonly bool _canFilterOutSourceItems;
        private readonly bool _bothSidesSupportCompositeChangeVectors;
        private readonly bool _senderIsHub;

        public FilteredReplicationDocumentSender(Stream stream, OutgoingPullReplicationHandler parent, RavenLogger log, string[] pathsToSend, string[] destinationAcceptablePaths) : base(stream, parent, log)
        {
            if (pathsToSend != null && pathsToSend.Length > 0)
                _pathsToSend = new AllowedPathsValidator(pathsToSend);
            if (destinationAcceptablePaths != null && destinationAcceptablePaths.Length > 0)
                _destinationAcceptablePaths = new AllowedPathsValidator(destinationAcceptablePaths);
            
            _shouldSkipSendingTombstones = parent.CanFilterOutSourceItemsByPreventingSinkToHubDeletions;
            _canFilterOutSourceItems = parent.CanFilterOutSourceItems;
            _bothSidesSupportCompositeChangeVectors = parent.BothSidesSupportCompositeChangeVectors;
            _senderIsHub = parent is OutgoingPullReplicationHandlerAsHub;
        }

        protected override void WriteReplicationItem(DocumentsOperationContext documentsContext, ReplicationBatchItem item, OutgoingReplicationStatsScope stats)
        {
            if (_bothSidesSupportCompositeChangeVectors)
            {
                WriteReplicationItemToStream(documentsContext, item, stats);
                return;
            }

            using (item.UseLegacyCompatibleChangeVectorsForSending(documentsContext, _senderIsHub))
            {
                WriteReplicationItemToStream(documentsContext, item, stats);
            }
        }

        protected override bool ShouldSkip(DocumentsOperationContext context, ReplicationBatchItem item, OutgoingReplicationStatsScope stats, SkippedReplicationItemsInfo skippedReplicationItemsInfo)
        {
            if (ValidatorSaysToSkip(_pathsToSend) || ValidatorSaysToSkip(_destinationAcceptablePaths))
                return true;

            if (_shouldSkipSendingTombstones && ReplicationLoader.IsOfTypePreventDeletions(item))
                return true;

            return base.ShouldSkip(context, item, stats, skippedReplicationItemsInfo);

            
            bool ValidatorSaysToSkip(AllowedPathsValidator validator)
            {
                if (validator == null)
                    return false;

                if (validator.ShouldAllow(item))
                    return false;

                stats.RecordArtificialDocumentSkip();
                skippedReplicationItemsInfo.Update(item);

                if (Log.IsDebugEnabled)
                {
                    string key = validator.GetItemInformation(item);
                    Log.Debug($"Will skip sending {key} ({item.Type}) because it was not allowed according to the incoming .");
                }

                return true;
            }
        }

        protected override void SendEmptyBatchHeartbeat(DocumentsOperationContext context, bool wasInterrupted, ChangeVector completedSourceFrontier)
        {
            if (wasInterrupted || _canFilterOutSourceItems == false)
            {
                base.SendEmptyBatchHeartbeat(context, wasInterrupted, completedSourceFrontier);
                return;
            }

            _parent.SendHeartbeat(databaseChangeVector: null, lastSentSourceChangeVector: completedSourceFrontier.AsString());
        }

        public override void Dispose()
        {
            _pathsToSend?.Dispose();
            _destinationAcceptablePaths?.Dispose();
            base.Dispose();
        }
    }
}
