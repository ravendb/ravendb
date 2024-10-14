using System;
using System.Collections.Generic;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Voron;

namespace Raven.Server.Documents.Revisions;

public partial class RevisionsStorage
{
    internal sealed class RevisionsBinCleanMergedCommand : DocumentMergedTransactionCommand
    {
        private IEnumerable<string> _revisionsBinEntriesIds;

        private readonly long _lastEtag;

        public (long DeletedEntries, bool CanContinueTransaction) Result { get; private set; }

        public RevisionsBinCleanMergedCommand(IEnumerable<string> revisionsBinEntriesIds, long lastEtag)
        {
            _revisionsBinEntriesIds = revisionsBinEntriesIds;
            _lastEtag = lastEtag;
        }

        protected override long ExecuteCmd(DocumentsOperationContext context)
        {
            Result = DeleteRevisions(context);
            
            SetLastRevisionsBinCleanerLastEtag(context, _lastEtag);
            return 1;
        }

        private (long DeletedRevisions, bool CanContinueTransaction) DeleteRevisions(DocumentsOperationContext context)
        {
            var revisionsStorage = context.DocumentDatabase.DocumentsStorage.RevisionsStorage;

            var deletedEntries = 0L;
            var canContinueTransaction = true;

            foreach (var id in _revisionsBinEntriesIds)
            {
                using (DocumentIdWorker.GetSliceFromId(context, id, out Slice lowerId))
                using (revisionsStorage.GetKeyPrefix(context, lowerId, out Slice prefixSlice))
                {
                    var collectionName = revisionsStorage.GetCollectionFor(context, prefixSlice);
                    if (collectionName == null)
                    {
                        if (revisionsStorage._logger.IsInfoEnabled)
                            revisionsStorage._logger.Info($"Tried to delete revisions for '{id}' but no revisions found.");
                        continue;
                    }

                    if (IsStillRevisionsBinEntry(context, lowerId, prefixSlice, collectionName, _lastEtag))
                    {
                        var result = revisionsStorage.ForceDeleteAllRevisionsFor(context, lowerId, prefixSlice, collectionName, Int64.MaxValue, shouldSkip: null);
                        if (result.MoreWork == false)
                        {
                            deletedEntries++;
                        }
                    }
                }

                if (context.CanContinueTransaction == false)
                {
                    canContinueTransaction = false;
                    break;
                }
            }

            return (deletedEntries, canContinueTransaction);
        }

        private bool IsStillRevisionsBinEntry(DocumentsOperationContext context, Slice lowerId, Slice prefixSlice, CollectionName collectionName, long lastEtag)
        {
            var lastRevision = context.DocumentDatabase.DocumentsStorage.RevisionsStorage.GetLastRevisionFor(context, lowerId, prefixSlice, collectionName, out _);
            return lastRevision != null && lastRevision.Flags.Contain(DocumentFlags.DeleteRevision) &&
                   ChangeVectorUtils.GetEtagById(lastRevision.ChangeVector, context.DocumentDatabase.DbBase64Id) <= lastEtag;
        }

        public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>>
            ToDto(DocumentsOperationContext context)
        {
            return new RevisionsBinCleanMergedCommandDto(_revisionsBinEntriesIds, _lastEtag);
        }

        public sealed class RevisionsBinCleanMergedCommandDto : IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, RevisionsBinCleanMergedCommand>
        {
            private IEnumerable<string> _revisionsBinEntitiesIds;

            private readonly long _lastEtag;

            public RevisionsBinCleanMergedCommandDto(IEnumerable<string> revisionsBinEntitiesIds, long lastEtag)
            {
                _revisionsBinEntitiesIds = revisionsBinEntitiesIds;
                _lastEtag = lastEtag;
            }

            public RevisionsBinCleanMergedCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
            {
                return new RevisionsBinCleanMergedCommand(_revisionsBinEntitiesIds, _lastEtag);
            }
        }
    }
}
