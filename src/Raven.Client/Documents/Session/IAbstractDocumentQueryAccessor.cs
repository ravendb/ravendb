using Raven.Client.Documents.Queries.Vector;

namespace Raven.Client.Documents.Session;

internal interface IAbstractDocumentQueryAccessor
{
    void VectorSearch(IVectorEmbeddingFieldFactoryAccessor fieldFactoryAccessor, IVectorFieldValueFactoryAccessor fieldValueFactoryAccessor, float? minimumSimilarity, int? numberOfCandidates, bool isExact);

    /// <summary>Opens a subclause that wraps a search statement, so it joins a preceding search with OR the way a bare search does.</summary>
    void OpenSearchSubclause();

    /// <summary>Adds AND when the query ends with a clause - also one a DocumentQuery held before ToQueryable().</summary>
    void AndAlsoIfNeeded();
}
