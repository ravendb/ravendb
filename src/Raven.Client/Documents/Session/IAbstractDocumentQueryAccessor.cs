using Raven.Client.Documents.Queries.Vector;

namespace Raven.Client.Documents.Session;

internal interface IAbstractDocumentQueryAccessor
{
    void VectorSearch(IVectorEmbeddingFieldFactoryAccessor fieldFactoryAccessor, IVectorFieldValueFactoryAccessor fieldValueFactoryAccessor, float? minimumSimilarity, int? numberOfCandidates, bool isExact);

    /// <summary>Opens a subclause that wraps a search statement, so it joins a preceding search with OR the way a bare search does.</summary>
    void OpenSubclause(bool isSearchStatement);
}
