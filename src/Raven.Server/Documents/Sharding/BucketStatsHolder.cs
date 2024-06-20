using System.Collections.Generic;
using System.Runtime.InteropServices;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Server;
using Voron;
using Voron.Data.BTrees;
using Voron.Data.Tables;
using Voron.Impl;

namespace Raven.Server.Documents.Sharding
{
    internal sealed unsafe class BucketStatsHolder
    {
        private readonly Dictionary<int, Documents.BucketStats> _values;
        private readonly Dictionary<int, ChangeVector> _mergedChangeVectors;

        private static readonly int BucketStatsSize = sizeof(Documents.BucketStats);

        public BucketStatsHolder()
        {
            _values = new Dictionary<int, Documents.BucketStats>();
            _mergedChangeVectors = new Dictionary<int, ChangeVector>();
        }

        public void UpdateBucket(int bucket, long nowTicks, long sizeChange, long numOfDocsChanged)
        {
            ref var bucketStats = ref CollectionsMarshal.GetValueRefOrAddDefault(_values, bucket, out _);

            bucketStats.Size += sizeChange;
            bucketStats.NumberOfDocuments += numOfDocsChanged;
            bucketStats.LastModifiedTicks = nowTicks;
        }

        public void UpdateBucketAndChangeVector(DocumentsOperationContext ctx, int bucket, long nowTicks, long sizeChange, long numOfDocsChanged, int changeVectorIndex, ref TableValueReader value)
        {
            UpdateBucket(bucket, nowTicks, sizeChange, numOfDocsChanged);

            var changeVector = DocumentsStorage.TableValueToChangeVector(ctx, changeVectorIndex, ref value);
            if (_mergedChangeVectors.TryGetValue(bucket, out var currentMergedCv))
            {
                changeVector = currentMergedCv.MergeWith(changeVector, ctx);
            }

            _mergedChangeVectors[bucket] = changeVector;
        }

        public void UpdateBucketStatsTreeBeforeCommit(Transaction tx)
        {
            var keyBuff = stackalloc byte[sizeof(int)];
            var tree = tx.ReadTree(ShardedDocumentsStorage.BucketStatsSlice);

            foreach ((int bucket, Documents.BucketStats inMemoryStats) in _values)
            {
                *(int*)keyBuff = bucket;

                using (Slice.External(tx.Allocator, keyBuff, sizeof(int), ByteStringType.Immutable, out var keySlice))
                using (MergeStats(tx, tree, inMemoryStats, keySlice, bucket, out var stats, out var mergedCv))
                {
                    if (stats.Size == 0 && stats.NumberOfDocuments == 0)
                    {
                        tree.Delete(keySlice);
                        continue;
                    }

                    using (tree.DirectAdd(keySlice, BucketStatsSize + mergedCv.Size, out var ptr))
                    {
                        *(Documents.BucketStats*)ptr = stats;
                        if (mergedCv.HasValue)
                            mergedCv.CopyTo(ptr + BucketStatsSize);
                    }
                }
            }

            _values.Clear();
            _mergedChangeVectors.Clear();
        }

        public void ClearBucketStatsOnFailure(LowLevelTransaction _)
        {
            _values.Clear();
            _mergedChangeVectors.Clear();
        }

        private ByteStringContext.InternalScope MergeStats(Transaction tx, Tree tree, Documents.BucketStats inMemoryStats, Slice keySlice, int bucket, out Documents.BucketStats stats, out Slice mergedCvSlice)
        {
            var ctx = tx.Owner as DocumentsOperationContext;
            string mergedCv = null;
            mergedCvSlice = Slices.Empty;

            var readResult = tree.Read(keySlice);
            if (readResult == null)
            {
                stats = inMemoryStats;
            }
            else
            {
                stats = *(Documents.BucketStats*)readResult.Reader.Base;
                stats.Size += inMemoryStats.Size;
                stats.NumberOfDocuments += inMemoryStats.NumberOfDocuments;
                stats.LastModifiedTicks = inMemoryStats.LastModifiedTicks;

                mergedCv = Documents.BucketStats.GetMergedChangeVector(readResult.Reader);
            }

            if (_mergedChangeVectors.TryGetValue(bucket, out var changeVector))
            {
                mergedCv = changeVector.MergeWith(mergedCv, ctx);
            }

            if (mergedCv == null) 
                return default;
            
            return Slice.From(tx.Allocator, mergedCv, out mergedCvSlice);
        }
    }
}
