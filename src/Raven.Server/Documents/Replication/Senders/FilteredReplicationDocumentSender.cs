using System.IO;
using Raven.Client.Documents.Operations.Replication;
using Raven.Server.Documents.Replication.Outgoing;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.Replication.Stats;
using Raven.Server.ServerWide.Context;
using Sparrow.Logging;

namespace Raven.Server.Documents.Replication.Senders
{
    public sealed class FilteredReplicationDocumentSender : ExternalReplicationDocumentSender
    {
        private readonly AllowedPathsValidator _pathsToSend, _destinationAcceptablePaths;
        private readonly bool _shouldSkipSendingTombstones;

        public FilteredReplicationDocumentSender(Stream stream, OutgoingPullReplicationHandler parent, Logger log, string[] pathsToSend, string[] destinationAcceptablePaths) : base(stream, parent, log)
        {
            if (pathsToSend != null && pathsToSend.Length > 0)
                _pathsToSend = new AllowedPathsValidator(pathsToSend);
            if (destinationAcceptablePaths != null && destinationAcceptablePaths.Length > 0)
                _destinationAcceptablePaths = new AllowedPathsValidator(destinationAcceptablePaths);
            
            _shouldSkipSendingTombstones = _parent.Destination is PullReplicationAsSink sink && sink.Mode == PullReplicationMode.SinkToHub && 
                                           _parent is OutgoingPullReplicationHandler pull &&
                                           pull.OutgoingPullReplicationParams?.PreventDeletionsMode?.HasFlag(PreventDeletionsMode.PreventSinkToHubDeletions) == true &&
                                           _parent._database.ForTestingPurposes?.ForceSendTombstones != true;
        }

        protected override bool ShouldSkip(DocumentsOperationContext context, ReplicationBatchItem item, OutgoingReplicationStatsScope stats, SkippedReplicationItemsInfo skippedReplicationItemsInfo)
        {
            if (ValidatorSaysToSkip(_pathsToSend) || ValidatorSaysToSkip(_destinationAcceptablePaths))
                return true;

            if (_shouldSkipSendingTombstones && ReplicationLoader.IsOfTypePreventDeletions(item))
                return true;

            var shouldSkip = base.ShouldSkip(context, item, stats, skippedReplicationItemsInfo);
            if (shouldSkip == false)
            {
                var current = context.GetChangeVector(item.ChangeVector);
                var database = _parent._database;
                var fromFilteredReplicationMarker = context.GetChangeVector(ChangeVectorParser.FilteredTag, item.Etag, database.DbBase64Id);

                // The filtered marker is delivery order only. Keep the real document lineage in Version only.
                item.ChangeVector = context.GetChangeVector(current.Version, fromFilteredReplicationMarker);
            }

            return shouldSkip;

            bool ValidatorSaysToSkip(AllowedPathsValidator validator)
            {
                if (validator == null)
                    return false;

                if (validator.ShouldAllow(item))
                    return false;

                stats.RecordArtificialDocumentSkip();
                skippedReplicationItemsInfo.Update(item);

                if (Log.IsInfoEnabled)
                {
                    string key = validator.GetItemInformation(item);
                    Log.Info($"Will skip sending {key} ({item.Type}) because it was not allowed according to the incoming .");
                }

                return true;
            }
        }

        public override void Dispose()
        {
            _pathsToSend?.Dispose();
            _destinationAcceptablePaths?.Dispose();
            base.Dispose();
        }
    }
}
