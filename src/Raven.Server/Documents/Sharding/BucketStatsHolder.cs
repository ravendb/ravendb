using System;
using System.Collections.Generic;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Voron;
using Voron.Data.Tables;
using Voron.Impl;

namespace Raven.Server.Documents.Sharding
{
    internal unsafe class BucketStatsHolder
    {
        private readonly ShardedDocumentsStorage _storage;
        private readonly Dictionary<int, Documents.BucketStats> _values;
        private readonly Dictionary<int, ChangeVector> _mergedChangeVectors;
        private DocumentsOperationContext _ctx;

        public BucketStatsHolder(ShardedDocumentsStorage storage)
        {
            _storage = storage;
            _values = new Dictionary<int, Documents.BucketStats>();
            _mergedChangeVectors = new Dictionary<int, ChangeVector>();
        }

        public void UpdateBucket(int bucket, long nowTicks, long sizeChange, long numOfDocsChanged)
        {
            _values.TryGetValue(bucket, out var bucketStats);

            bucketStats.Size += sizeChange;
            bucketStats.NumberOfDocuments += numOfDocsChanged;
            bucketStats.LastModifiedTicks = nowTicks;

            _values[bucket] = bucketStats;
        }

        public void UpdateBucket(int bucket, long nowTicks, long sizeChange, long numOfDocsChanged, int changeVectorIndex, ref TableValueReader value)
        {
            UpdateBucket(bucket, nowTicks, sizeChange, numOfDocsChanged);

            var changeVector = TableValueToChangeVector(changeVectorIndex, ref value);
            if (_mergedChangeVectors.TryGetValue(bucket, out var currentMergedCv))
            {
                changeVector = currentMergedCv.MergeWith(changeVector, _ctx);
            }

            _mergedChangeVectors[bucket] = changeVector;
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

                        cvScope = stats.GetMergedChangeVector(tx.Allocator, readResult.Reader, out cvSlice);
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
                            changeVector = changeVector.MergeWith(cvSlice.HasValue ? cvSlice.ToString() : null, _ctx);
                            cvScope = Slice.From(tx.Allocator, changeVector, out cvSlice);
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

            _ctx?.Dispose();
            _ctx = null;
            _values.Clear();
            _mergedChangeVectors.Clear();
        }

        private ChangeVector TableValueToChangeVector(int changeVectorIndex, ref TableValueReader value)
        {
            if (_ctx == null)
                _storage.ContextPool.AllocateOperationContext(out _ctx);

            return DocumentsStorage.TableValueToChangeVector(_ctx, changeVectorIndex, ref value);
        }
    }
}
