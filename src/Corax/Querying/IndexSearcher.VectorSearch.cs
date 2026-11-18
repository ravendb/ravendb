using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using Corax.Indexing;
using Corax.Mappings;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Primitives;
using Corax.Utils;
using Voron.Data.RoaringBitmaps;
using Sparrow;
using Sparrow.Server.Utils;
using Voron;
using Voron.Data.CompactTrees;
using Voron.Data.Graphs;
using Voron.Data.Lookups;
using Voron.Impl;
using Voron.Util;

namespace Corax.Querying;

public partial class IndexSearcher
{
    internal static class VectorSearchUtils
    {
        public static bool ShouldScan(IndexSearcher indexSearcher, long filterMatchesCount, bool isExact, IQueryMatch filterQuery, int scanningThreshold, int numberOfCandidates)
        {
            var shouldScan = filterQuery != null && (filterMatchesCount < scanningThreshold || isExact || filterMatchesCount * 0.5 < numberOfCandidates);
            if (indexSearcher._testingConfiguration is {DisableVectorSearchScanning: true})
                return false;
             
            return shouldScan;
        }

        public static RoaringBitmap LoadFilterMatches(IndexSearcher indexSearcher, ref IQueryMatch query, out bool owned)
        {
            if (query is IBitmapQueryMatch bitmapMatch) // fast path, when source already has a bitmap
            {
                owned = false;
                ref RoaringBitmap borrowed = ref bitmapMatch.BitmapState;
                borrowed.PrepareForReading();
                return borrowed;
            }

            owned = true;
            RoaringBitmap filter = new(indexSearcher.Allocator);

            using var _ = indexSearcher.Allocator.Allocate(4096, out Span<long> workingBuffer);
            int read;
            while ((read = query.Fill(workingBuffer)) > 0)
            {
                for (int i = 0; i < read; i++)
                    filter.Add(workingBuffer[i]);
            }

            filter.PrepareForReading();
            return filter;
        }

        /// <summary>
        /// Materializes all entry IDs from a RoaringBitmap filter, shuffles them for random access,
        /// and converts each entry to its corresponding HNSW node ID(s) on demand.
        /// Designed for probing random starting nodes during approximate filtered nearest neighbor search.
        /// </summary>
        [System.Runtime.CompilerServices.SkipLocalsInit]
        public struct RandomNodesFromFilterEnumerator : IEnumerator<long>
        {
            private List<long> _results;
            private long _current;
            private readonly IndexSearcher _indexSearcher;
            private readonly long[] _entryIds;
            private int _entryIndex;
            private bool _isDone;
            private Page p = default;
            private CompactKey _key;
            private readonly CompactTree _vectorsByHash;
            private readonly Lookup<Int64LookupKey> _nodesByVectorId;
            private readonly long _vectorRootPage;

            public RandomNodesFromFilterEnumerator(IndexSearcher indexSearcher, FieldMetadata metadata, RoaringBitmap filterResults, Random random = null)
            {
                _indexSearcher = indexSearcher;
                _key = indexSearcher._transaction.LowLevelTransaction.AcquireCompactKey();
                var searchState = new Hnsw.SearchState(indexSearcher.Transaction.LowLevelTransaction, metadata.FieldName);
                indexSearcher._transaction.TryGetCompactTreeFor(Hnsw.VectorsIdByHashSlice, out _vectorsByHash);
                _nodesByVectorId = searchState.NodeIdsByVectorId;
                _current = -1L;

                var filterCount = filterResults.ComputeCount();
                _isDone = indexSearcher.TryGetRootPageByFieldName(metadata.FieldName, out _vectorRootPage) == false
                          || filterCount == 0;

                if (_isDone)
                {
                    _entryIds = Array.Empty<long>();
                    return;
                }

                const int maxFilterSampleSize = 8192;
                random ??= Random.Shared;
                var sampleSize = (int)Math.Min(filterCount, maxFilterSampleSize);
                _entryIds = new long[sampleSize];
                var ranks = new long[sampleSize];
                if (filterCount <= maxFilterSampleSize)
                {
                    // small enough that we can just take it all 
                    for (int i = 0; i < sampleSize; i++)
                        ranks[i] = i;
                }
                else
                {
                    // select random matches from the filter to fit the sample size
                    for (int i = 0; i < sampleSize; i++)
                        ranks[i] = random.NextInt64(filterCount);
                }

                filterResults.Select(_indexSearcher.Allocator, ranks, _entryIds);

                // ensure we scan in random order
                random.Shuffle(_entryIds.AsSpan());
                _entryIndex = 0;
            }

            public RandomNodesFromFilterEnumerator() => throw new NotSupportedException($"Default constructor is not supported for {nameof(RandomNodesFromFilterEnumerator)}");

            public void Dispose()
            {
                _current = -1;
                _isDone = true;
                _indexSearcher._transaction.LowLevelTransaction.ReleaseCompactKey(ref _key);
            }

            public bool MoveNext()
            {
                if (_isDone)
                    return false;

                if (_results is not null && _results.Count > 0)
                {
                    _current = _results[^1];
                    _results.RemoveAt(_results.Count - 1);
                    return true;
                }

                _current = -1L;
                while (_entryIndex < _entryIds.Length)
                {
                    var entryId = _entryIds[_entryIndex++];

                    var entryTermsReader = _indexSearcher.GetEntryTermsReader(entryId, ref p, _key);
                    bool found = false;
                    while (entryTermsReader.FindNextStored(_vectorRootPage))
                    {
                        var vectorHash = entryTermsReader.StoredField.Value;
                        var vectorExists = _vectorsByHash.TryGetValue(vectorHash, out var vectorId);
                        Debug.Assert(vectorExists, "Vector hash not found in vectors by hash tree");
                        var nodeIdExists = _nodesByVectorId.TryGetValue(vectorId, out var nodeId);
                        Debug.Assert(nodeIdExists, "Node ID not found in nodes by vector ID tree");
                        found = true;
                        if (_current == -1L)
                        {
                            _current = nodeId;
                        }
                        else
                        {
                            _results ??= new();
                            _results.Add(nodeId);
                        }
                    }

                    if (found)
                        return true;
                }

                _isDone = true;
                return false;
            }

            public void Reset()
            {
                throw new NotSupportedException($"Reset is not supported for {nameof(RandomNodesFromFilterEnumerator)}");
            }

            public long Current => _current;

            object IEnumerator.Current
            {
                get => Current;
            }
        }

        public static bool TryConvertDocumentsIdsToNodesIds(IndexSearcher indexSearcher, in FieldMetadata metadata, ref RoaringBitmap filterResults, out ContextBoundNativeList<long> nodesIdsToScan)
        {
            var searchState = new Hnsw.SearchState(indexSearcher.Transaction.LowLevelTransaction, metadata.FieldName);
            indexSearcher._transaction.TryGetCompactTreeFor(Hnsw.VectorsIdByHashSlice, out var vectorsByHash);
            var nodesByVectorId = searchState.NodeIdsByVectorId;
            if (indexSearcher.TryGetRootPageByFieldName(metadata.FieldName, out var vectorRootPage) is false)
            {
                nodesIdsToScan = default;
                return false;
            }

            nodesIdsToScan = new ContextBoundNativeList<long>(indexSearcher.Allocator);

            Page p = default;
            using var iterator = filterResults.GetIterator();
            Span<long> batch = stackalloc long[QueryPrimitives.EntryScanBatchSize];
            int read;
            while ((read = iterator.Fill(ref filterResults, batch)) > 0)
            {
                for (int i = 0; i < read; i++)
                {
                    var entryTermsReader = indexSearcher.GetEntryTermsReader(batch[i], ref p);
                    while (entryTermsReader.FindNextStored(vectorRootPage))
                    {
                        var vectorHash = entryTermsReader.StoredField.Value;
                        if (vectorsByHash.TryGetValue(vectorHash, out var vectorId))
                        {
                            if (nodesByVectorId.TryGetValue(vectorId, out var nodeId))
                                nodesIdsToScan.Add(nodeId);
                        }
                    }
                }
            }
            // we get _all_ the node ids, so we can then use exact nearest neighbour to find the closest matches 
            var uniqueCount = Sorting.SortAndRemoveDuplicates(nodesIdsToScan.ToSpan());
            nodesIdsToScan.Count = uniqueCount;

            if (nodesIdsToScan.Count == 0)
            {
                nodesIdsToScan.Dispose();
                nodesIdsToScan = default;
                return false;
            }

            return nodesIdsToScan.Count > 0;
        }
    }


    public VectorSearchMatch VectorSearch(in FieldMetadata metadata, in VectorValue vectorValue, float minimumMatch, in int numberOfCandidates, bool isExact, bool sortByScore, IQueryMatch filterQuery = null, int scanningThreshold = 1024, Random random = null)
    {
        return new VectorSearchMatch(this, metadata, vectorValue, minimumMatch, numberOfCandidates, isExact, sortByScore, filterQuery, scanningThreshold, random);
    }

    public IQueryMatch VectorSearch(in FieldMetadata metadata, in string documentId, float minimumMatch, in int numberOfCandidates, bool isExact, bool sortByScore, IQueryMatch filterQuery = null, int scanningThreshold = 1024)
    {
        var idField = GetTermsFor(_fieldMapping.GetByFieldId(Constants.IndexWriter.PrimaryKeyFieldId).FieldName);
        string loweredDocumentId = documentId.ToLowerInvariant();
        if (idField == null ||
            idField.TryGetValue(loweredDocumentId, out var rawId) is false ||
            TryGetRootPageByFieldName(metadata.FieldName, out var vectorRootPage) is false ||
            _transaction.TryGetCompactTreeFor(Hnsw.VectorsIdByHashSlice, out var vectorsByHash) is false)
            return EmptyMatch();
        PortableExceptions.ThrowIf<InvalidOperationException>((rawId & (long)TermIdMask.EnsureIsSingleMask) != (long)TermIdMask.Single,
            "The provided id must be a document id mapped to a single value, but got: " + documentId + ", which maps to: " + rawId);

        Page page = default;
        var singleEntryId = EntryIdEncodings.GetContainerId(rawId);
        var reader = GetEntryTermsReader((long)singleEntryId, ref page);

        var searchState = new Hnsw.SearchState(_transaction.LowLevelTransaction, metadata.FieldName);

        if (reader.FindNextStored(vectorRootPage) is false)
            return EmptyMatch();

        PortableExceptions.ThrowIf<InvalidOperationException>(reader.IsVectorHash is false, "Expected vector field, but got " + metadata.FieldName + ", which isn't a vector");

        Span<byte> hash = reader.StoredField.Value.ToSpan();
        if (vectorsByHash.TryGetValue(hash, out var vectorId) is false)
            return EmptyMatch();

        var vectorSpan = Hnsw.NodeReader.ReadVector(vectorId, searchState);

        var vectorValue = new VectorValue(null, vectorSpan.AsMemory());
        if (reader.FindNextStored(vectorRootPage) is false) // just a single vector
            return new VectorSearchMatch(this, metadata, vectorValue, minimumMatch, numberOfCandidates, isExact, sortByScore, filterQuery, scanningThreshold);

        List<VectorValue> vectors = [vectorValue];
        do
        {
            vectorSpan = Hnsw.NodeReader.ReadVector(vectorId, searchState);
            vectorValue = new VectorValue(null, vectorSpan.AsMemory());
            vectors.Add(vectorValue);
        } while (reader.FindNextStored(vectorRootPage));

        return new MultiVectorSearchMatch(this, metadata, vectors.ToArray(), minimumMatch, numberOfCandidates, isExact, sortByScore, filterQuery, scanningThreshold);
    }

    public MultiVectorSearchMatch MultiVectorSearch(in FieldMetadata metadata, in VectorValue[] vectorValues, float minimumMatch, in int numberOfCandidates, bool isExact, bool sortByScore, IQueryMatch filterQuery = null, int scanningThreshold = 1024, Random random = null)
        => new(this, metadata, vectorValues, minimumMatch, numberOfCandidates, isExact, sortByScore, filterQuery, scanningThreshold, random);
}
