using System;
using System.Collections.Generic;
using Elastic.Clients.Elasticsearch;
using Raven.Server.Documents.Sharding;
using Raven.Server.ServerWide.Context;
using Sparrow.Server;
using Voron;
using Voron.Data.Tables;
using Voron.Impl;
using static Raven.Server.Documents.Schemas.Revisions;

namespace Raven.Server.Documents.Revisions
{
    public partial class RevisionsStorage
    {
        public IEnumerable<Document> GetRevisionsByBucketFrom(DocumentsOperationContext context, int bucket, long etag)
        {
            var table = new Table(RevisionsSchema, context.Transaction.InnerTransaction);

            foreach (var result in ShardedDocumentsStorage.GetItemsByBucket(context.Allocator, table, RevisionsSchema.DynamicKeyIndexes[RevisionsBucketAndEtagSlice], bucket, etag))
            {
                yield return TableValueToRevision(context, ref result.Result.Reader);
            }
        }

        [StorageIndexEntryKeyGenerator]
        internal static ByteStringContext.Scope GenerateBucketAndEtagIndexKeyForRevisions(Transaction tx, ref TableValueReader tvr, out Slice slice)
        {
            return ShardedDocumentsStorage.GenerateBucketAndEtagIndexKey(tx, idIndex: (int)RevisionsTable.LowerId, etagIndex: (int)RevisionsTable.Etag, ref tvr, out slice);
        }

        internal static void UpdateBucketStatsForRevisions(Transaction tx, Slice key, ref TableValueReader oldValue, ref TableValueReader newValue)
        {
            ShardedDocumentsStorage.UpdateBucketStatsInternal(tx, key, ref newValue, changeVectorIndex: (int)RevisionsTable.ChangeVector, sizeChange: newValue.Size - oldValue.Size);
        }

        public void ForceDeleteAllRevisionsFor(DocumentsOperationContext context, string id)
        {
            using (DocumentIdWorker.GetSliceFromId(context, id, out Slice lowerId))
            using (GetKeyPrefix(context, lowerId, out Slice prefixSlice))
            {
                var collectionName = GetCollectionFor(context, prefixSlice);
                if (collectionName == null)
                {
                    if (_logger.IsInfoEnabled)
                        _logger.Info($"Tried to delete all revisions for '{id}' but no revisions found.");
                    return;
                }

                ForceDeleteAllRevisionsForInternal(context, lowerId, prefixSlice, collectionName, shouldSkip: null, maxDeletesUponUpdate: null, tombstoneFlags: DocumentFlags.FromResharding | DocumentFlags.Artificial);
            }
        }

    }
}
