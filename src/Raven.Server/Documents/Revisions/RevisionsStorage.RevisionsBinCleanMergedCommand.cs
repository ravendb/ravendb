using System;
using System.Collections.Generic;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Voron;

namespace Raven.Server.Documents.Revisions;

public partial class RevisionsStorage
{
    internal sealed class RevisionsBinCleanMergedCommand : DocumentMergedTransactionCommand
    {
        private List<(string Id, long Etag)> _idsAndEtags;
        private readonly long _lastEtag;
        private readonly bool _isFirst;

        private const int MaxDeletesUponUpdate = 1024;

        public (int DeletedEntries, int NextStartIndex) Result { get; private set; }

        public RevisionsBinCleanMergedCommand(List<(string Id, long Etag)> idsAndEtags, long lastEtag, bool isFirst)
        {
            _idsAndEtags = idsAndEtags;
            _lastEtag = lastEtag;
            _isFirst = isFirst;
        }

        protected override long ExecuteCmd(DocumentsOperationContext context)
        {
            Result = DeleteRevisions(context);
            var etag = Result.NextStartIndex == _idsAndEtags.Count ? _lastEtag : _idsAndEtags[Result.NextStartIndex].Etag;
            SetLastRevisionsBinCleanerLastEtag(context, etag);
            return 1;
        }

        private (int DeletedEntries, int NextStartIndex) DeleteRevisions(DocumentsOperationContext context)
        {
            var revisionsStorage = context.DocumentDatabase.DocumentsStorage.RevisionsStorage;

            var index = 0;
            var deletedEntities = 0;

            foreach (var (id, etag) in _idsAndEtags)
            {
                using (DocumentIdWorker.GetSliceFromId(context, id, out Slice lowerId))
                using (revisionsStorage.GetKeyPrefix(context, lowerId, out Slice prefixSlice))
                {
                    var collectionName = revisionsStorage.GetCollectionFor(context, prefixSlice);
                    if (collectionName == null)
                    {
                        if (_isFirst && revisionsStorage._logger.IsInfoEnabled)
                            revisionsStorage._logger.Info($"Tried to delete revisions for '{id}' but no revisions found.");
                    }
                    else
                    {
                        using var document = context.DocumentDatabase.DocumentsStorage.Get(context, lowerId, fields: DocumentFields.Default, throwOnConflict: false);
                        if (document == null) // document is delete, so we can remove all its revisions
                        {
                            if (_isFirst == false)
                                return (deletedEntities, index);

                            (bool moreWork, long _) =
                                revisionsStorage.ForceDeleteAllRevisionsFor(context, lowerId, prefixSlice, collectionName, MaxDeletesUponUpdate, etagBarrier: etag);
                            if (moreWork)
                                return (deletedEntities, index);

                            deletedEntities++;
                        }
                    }
                }

                index++;
            }

            return (deletedEntities, index);
        }

        public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>>
            ToDto(DocumentsOperationContext context)
        {
            return new RevisionsBinCleanMergedCommandDto(_idsAndEtags, _lastEtag, _isFirst);
        }

        public sealed class RevisionsBinCleanMergedCommandDto : IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, RevisionsBinCleanMergedCommand>
        {
            private List<(string Id, long Etag)> _idsAndEtags;
            private readonly long _lastEtag;
            private readonly bool _isFirst;

            public RevisionsBinCleanMergedCommandDto(List<(string Id, long Etag)> idsAndEtags, long lastEtag, bool isFirst)
            {
                _idsAndEtags = idsAndEtags;
                _lastEtag = lastEtag;
                _isFirst = isFirst;
            }

            public RevisionsBinCleanMergedCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
            {
                return new RevisionsBinCleanMergedCommand(_idsAndEtags, _lastEtag, _isFirst);
            }
        }
    }
}
