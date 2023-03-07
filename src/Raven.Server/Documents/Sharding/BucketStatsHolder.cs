using System;
using System.Collections.Generic;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Voron;
using Voron.Impl;

namespace Raven.Server.Documents.Sharding
{
    internal unsafe class BucketStatsHolder
    {
        private readonly Dictionary<int, Documents.BucketStats> _values = new();
        private readonly Dictionary<int, ChangeVector> _mergedChangeVectors = new();

        public void UpdateBucket(IChangeVectorOperationContext ctx, int bucket, long nowTicks, long sizeChange, long numOfDocsChanged, ChangeVector changeVector)
        {
            UpdateBucket(bucket, nowTicks, sizeChange, numOfDocsChanged);

            ChangeVector mergedCv;
            if (_mergedChangeVectors.TryGetValue(bucket, out var currentMergedCv) == false)
            {
                mergedCv = changeVector;
            }
            else
            {
                var mergedCvStr = ChangeVectorUtils.MergeVectors(currentMergedCv, changeVector);
                mergedCv = ctx.GetChangeVector(mergedCvStr);
            }

            _mergedChangeVectors[bucket] = mergedCv;
        }

        public void UpdateBucket(int bucket, long nowTicks, long sizeChange, long numOfDocsChanged)
        {
            _values.TryGetValue(bucket, out var bucketStats);

            bucketStats.Size += sizeChange;
            bucketStats.NumberOfDocuments += numOfDocsChanged;
            bucketStats.LastModifiedTicks = nowTicks;

            _values[bucket] = bucketStats;
        }

        public void UpdateBucketStatsTreeBeforeCommit(Transaction tx)
        {
            int bucketStatsSize = sizeof(Documents.BucketStats);
            var tree = tx.ReadTree(ShardedDocumentsStorage.BucketStatsSlice);

            foreach ((int bucket, Documents.BucketStats inMemoryStats) in _values)
            {
                using (tx.Allocator.Allocate(sizeof(int), out var keyBuffer))
                {
                    *(int*)keyBuffer.Ptr = bucket;
                    var keySlice = new Slice(keyBuffer);
                    var readResult = tree.Read(keySlice);

                    Documents.BucketStats stats;
                    IDisposable cvScope = null;
                    Slice cvSlice = default;

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

                        if (readResult.Reader.Length > bucketStatsSize)
                        {
                            cvScope = Slice.From(tx.Allocator, readResult.Reader.Base + bucketStatsSize, readResult.Reader.Length - bucketStatsSize, out cvSlice);
                        }
                    }

                    if (stats.Size == 0 && stats.NumberOfDocuments == 0)
                    {
                        tree.Delete(keySlice);
                        continue;
                    }

                    if (_mergedChangeVectors.TryGetValue(bucket, out var changeVector))
                    {
                        using (cvScope)
                        {
                            // update merged change vector
                            var mergedCv = cvSlice.HasValue
                                ? ChangeVectorUtils.MergeVectors(cvSlice.ToString(), changeVector)
                                : changeVector;
                            cvScope = Slice.From(tx.Allocator, mergedCv, out cvSlice);
                        }
                    }

                    using (cvScope)
                    using (tree.DirectAdd(keySlice, bucketStatsSize + cvSlice.Size, out var ptr))
                    {
                        *(Documents.BucketStats*)ptr = stats;
                        if (cvSlice.HasValue)
                            cvSlice.CopyTo(ptr + bucketStatsSize);
                    }
                }
            }

            _values.Clear();
            _mergedChangeVectors.Clear();
        }
    }
}
