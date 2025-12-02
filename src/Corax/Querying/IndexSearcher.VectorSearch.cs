using System;
using System.Collections.Generic;
using Corax.Indexing;
using Corax.Mappings;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Utils;
using Sparrow;
using Sparrow.Server;
using Sparrow.Server.Collections;
using Sparrow.Server.Utils;
using Voron;
using Voron.Data.CompactTrees;
using Voron.Data.Containers;
using Voron.Data.Graphs;
using Voron.Util;

namespace Corax.Querying;

public partial class IndexSearcher
{
    internal static class VectorSearchUtils
    {
        public static bool ShouldScan(IndexSearcher indexSearcher, long filterMatchesCount, bool isExact, IQueryMatch filterQuery, int scanningThreshold) => filterQuery != null && (filterMatchesCount < scanningThreshold || isExact);

        public static GrowableBitArray LoadFilterMatches(IndexSearcher indexSearcher, ref IQueryMatch query)
        {
            using var _ = indexSearcher.Allocator.Allocate(1024, out Span<long> workingBuffer);

            var totalCount = 0;
            GrowableBitArray filter = new(indexSearcher.Allocator, indexSearcher.LastEntryId);
            while (query.Fill(workingBuffer) is var read and > 0)
            {
                for (int i = 0; i < read; ++i)
                {
                    totalCount += filter.Add(workingBuffer[i]).ToInt32();
                }
            }

            filter.Count = totalCount;
            return filter;
        }

        public static bool TryConvertDocumentsIdsToNodesIds(IndexSearcher indexSearcher, in FieldMetadata metadata, GrowableBitArray filterResults, out ContextBoundNativeList<long> nodesIdsToScan)
        {
            var searchState = new Hnsw.SearchState(indexSearcher.Transaction.LowLevelTransaction, metadata.FieldName);
            var vectorsByHash = indexSearcher._transaction.CompactTreeFor(Hnsw.VectorsIdByHashSlice);
            var nodesByVectorId = searchState.NodeIdsByVectorId;
            if (indexSearcher.TryGetRootPageByFieldName(metadata.FieldName, out var vectorRootPage) is false)
            {
                nodesIdsToScan = default;
                return false;
            }

            nodesIdsToScan = new ContextBoundNativeList<long>(indexSearcher.Allocator);

            // Scan all entries from the filter to retrieve all node IDs. This is important for returning the correct number of results.
            // To satisfy the NumberOfCandidates requirement, we need to return up to `NumberOfCandidates` posting lists with the filter applied to the query.
            // Instead of building a mapping of nodes to matching posting lists (and distances),
            // we reuse ExactSearch mechanisms to find the nearest nodes to the vector (so we know that each node has at least one matching document)
            // and then filter the documents stored in the posting list of each node individually.
            // Ideally, each node represents only a single document.
            Page p = default;
            foreach (var docId in filterResults.Iterate(0))
            {
                var entryTermsReader = indexSearcher.GetEntryTermsReader(docId, ref p);
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

            var uniqueCount = Sorting.SortAndRemoveDuplicates(nodesIdsToScan.ToSpan());
            nodesIdsToScan.Count = uniqueCount;
            return true;
        }
    }


    public VectorSearchMatch VectorSearch(in FieldMetadata metadata, in VectorValue vectorValue, float minimumMatch, in int numberOfCandidates, bool isExact, bool isSingleVectorSearch, IQueryMatch filterQuery = null, int scanningThreshold = 1024)
    {
        return new VectorSearchMatch(this, metadata, vectorValue, minimumMatch, numberOfCandidates, isExact, isSingleVectorSearch, filterQuery, scanningThreshold);
    }

    public IQueryMatch VectorSearch(in FieldMetadata metadata, in string documentId, float minimumMatch, in int numberOfCandidates, bool isExact, bool isSingleVectorSearch, IQueryMatch filterQuery = null, int scanningThreshold = 1024)
    {
        var idField = _fieldsTree.CompactTreeFor(_fieldMapping.GetByFieldId(Constants.IndexWriter.PrimaryKeyFieldId).FieldName);
        string loweredDocumentId = documentId.ToLowerInvariant();
        if (idField.TryGetValue(loweredDocumentId, out var rawId) is false ||
            TryGetRootPageByFieldName(metadata.FieldName, out var vectorRootPage) is false)
            return EmptyMatch();
        var vectorsByHash = _transaction.CompactTreeFor(Hnsw.VectorsIdByHashSlice);
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
            return new VectorSearchMatch(this, metadata, vectorValue, minimumMatch, numberOfCandidates, isExact, isSingleVectorSearch, filterQuery, scanningThreshold);

        List<VectorValue> vectors = [vectorValue];
        do
        {
            vectorSpan = Hnsw.NodeReader.ReadVector(vectorId, searchState);
            vectorValue = new VectorValue(null, vectorSpan.AsMemory());
            vectors.Add(vectorValue);
        } while (reader.FindNextStored(vectorRootPage));

        return new MultiVectorSearchMatch(this, metadata, vectors.ToArray(), minimumMatch, numberOfCandidates, isExact, isSingleVectorSearch, filterQuery, scanningThreshold);
    }

    public MultiVectorSearchMatch MultiVectorSearch(in FieldMetadata metadata, in VectorValue[] vectorValues, float minimumMatch, in int numberOfCandidates, bool isExact, bool isSingleVectorSearch, IQueryMatch filterQuery = null, int scanningThreshold = 1024)
        => new(this, metadata, vectorValues, minimumMatch, numberOfCandidates, isExact, isSingleVectorSearch, filterQuery, scanningThreshold);
}
