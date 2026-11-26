using Raven.Client.Documents.Queries.Vector;

namespace Raven.Client.Documents.Session;

internal interface IAbstractDocumentQueryAccessor
{
    void VectorSearch(IVectorEmbeddingFieldFactoryAccessor fieldFactoryAccessor, IVectorFieldValueFactoryAccessor fieldValueFactoryAccessor, float? minimumSimilarity, int? numberOfCandidates, bool isExact);

    /// <summary>Opens a subclause that wraps a search statement, so it joins a preceding search with OR the way a bare search does.</summary>
    void OpenSearchSubclause();

    /// <summary>Adds AND when the query ends with a clause - also one a DocumentQuery held before ToQueryable().</summary>
    void AndAlsoIfNeeded();

    /// <summary>Adds AND to the query; if the previous clauses contain an OR outside parentheses, wraps them first so the AND applies to all of them.</summary>
    void AndAlsoWrappingPreviousOrIfNeeded();
}
