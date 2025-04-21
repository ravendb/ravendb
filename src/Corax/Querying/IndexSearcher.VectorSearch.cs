using System;
using System.Collections.Generic;
using Corax.Indexing;
using Corax.Mappings;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Utils;
using Sparrow;
using Sparrow.Server;
using Voron;
using Voron.Data.CompactTrees;
using Voron.Data.Containers;
using Voron.Data.Graphs;

namespace Corax.Querying;

public partial class IndexSearcher
{
    public VectorSearchMatch VectorSearch(in FieldMetadata metadata, in VectorValue vectorValue, float minimumMatch, in int numberOfCandidates, bool isExact, bool isSingleVectorSearch)
    {
        return new VectorSearchMatch(this, metadata, vectorValue, minimumMatch, numberOfCandidates, isExact, isSingleVectorSearch);
    }
    
    public IQueryMatch VectorSearch(in FieldMetadata metadata, in string documentId, float minimumMatch, in int numberOfCandidates, bool isExact, bool isSingleVectorSearch)
    {
        var idField = _fieldsTree.CompactTreeFor(_fieldMapping.GetByFieldId(Constants.IndexWriter.PrimaryKeyFieldId).FieldName);
        string loweredDocumentId = documentId.ToLowerInvariant();
        if (idField.TryGetValue(loweredDocumentId, out var rawId) is false || 
            TryGetRootPageByFieldName(metadata.FieldName, out var vectorRootPage) is false)
            return EmptyMatch();
        var vectorsByHash = _transaction.CompactTreeFor(Hnsw.VectorsIdByHashSlice);
        PortableExceptions.ThrowIf<InvalidOperationException>((rawId & (long)TermIdMask.EnsureIsSingleMask) != (long)TermIdMask.Single,
            "The provided id must be a document id mapped to a single value, but got: " + documentId +", which maps to: " + rawId); 

        Page page = default;
        var singleEntryId = EntryIdEncodings.GetContainerId(rawId);
        var reader = GetEntryTermsReader(singleEntryId, ref page);

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
            return new VectorSearchMatch(this, metadata, vectorValue, minimumMatch, numberOfCandidates, isExact, isSingleVectorSearch);

        List<VectorValue> vectors = [vectorValue]; 
        do
        {
            vectorSpan = Hnsw.NodeReader.ReadVector(vectorId, searchState);
            vectorValue = new VectorValue(null, vectorSpan.AsMemory());
            vectors.Add(vectorValue);
        } while (reader.FindNextStored(vectorRootPage));
        
        return new MultiVectorSearchMatch(this, metadata, vectors.ToArray(), minimumMatch, numberOfCandidates, isExact, isSingleVectorSearch);
    }


    public MultiVectorSearchMatch MultiVectorSearch(in FieldMetadata metadata, in VectorValue[] vectorValues, float minimumMatch, in int numberOfCandidates, bool isExact,
        bool isSingleVectorSearch) => new MultiVectorSearchMatch(this, metadata, vectorValues, minimumMatch, numberOfCandidates, isExact, isSingleVectorSearch);
}
