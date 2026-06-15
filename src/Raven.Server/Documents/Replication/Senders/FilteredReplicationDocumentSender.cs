using System;
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
        private readonly OutgoingPullReplicationHandler _pullReplicationHandler;

        public FilteredReplicationDocumentSender(Stream stream, OutgoingPullReplicationHandler parent, RavenLogger log, string[] pathsToSend, string[] destinationAcceptablePaths) : base(stream, parent, log)
        {
            _pullReplicationHandler = parent;

            if (pathsToSend != null && pathsToSend.Length > 0)
                _pathsToSend = new AllowedPathsValidator(pathsToSend);

            if (destinationAcceptablePaths != null && destinationAcceptablePaths.Length > 0)
                _destinationAcceptablePaths = new AllowedPathsValidator(destinationAcceptablePaths);
        }

        protected override void WriteReplicationItem(DocumentsOperationContext documentsContext, ReplicationBatchItem item, OutgoingReplicationStatsScope stats)
        {
            switch (_pullReplicationHandler.ChangeVectorTransmission)
            {
                case PullReplicationChangeVectorTransmission.SendAsIs:
                    break;

                case PullReplicationChangeVectorTransmission.SendVersionOnly:
                    item.ChangeVector = documentsContext.GetChangeVector(item.ChangeVector).Version;

                    var timeSeriesItem = item as TimeSeriesReplicationItem;
                    if (timeSeriesItem != null)
                        timeSeriesItem.ParentDocChangeVector = documentsContext.GetChangeVector(timeSeriesItem.ParentDocChangeVector).Version;

                    if (_pullReplicationHandler is OutgoingPullReplicationHandlerAsHub)
                    {
                        var hubDatabaseChangeVector = documentsContext.LastDatabaseChangeVector ?? DocumentsStorage.GetDatabaseChangeVector(documentsContext);
                        item.ChangeVector = ChangeVectorUtils.MaskUnknownEntriesWithSinkTag(documentsContext, item.ChangeVector, hubDatabaseChangeVector);

                        if (timeSeriesItem != null)
                            timeSeriesItem.ParentDocChangeVector = ChangeVectorUtils.MaskUnknownEntriesWithSinkTag(documentsContext, timeSeriesItem.ParentDocChangeVector, hubDatabaseChangeVector);
                    }
                    break;

                default:
                    throw new ArgumentOutOfRangeException(nameof(_pullReplicationHandler.ChangeVectorTransmission), _pullReplicationHandler.ChangeVectorTransmission, "Unknown pull replication change-vector transmission.");
            }

            base.WriteReplicationItem(documentsContext, item, stats);
        }

        protected override bool ShouldSkip(DocumentsOperationContext context, ReplicationBatchItem item, OutgoingReplicationStatsScope stats, SkippedReplicationItemsInfo skippedReplicationItemsInfo)
        {
            if (ValidatorSaysToSkip(_pathsToSend) || ValidatorSaysToSkip(_destinationAcceptablePaths))
                return true;

            if (_pullReplicationHandler.CanFilterOutSourceItemsByPreventingSinkToHubDeletions && item.IsPreventableSinkToHubDeletion())
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
            if (wasInterrupted || _pullReplicationHandler.CanFilterOutSourceItems == false)
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
