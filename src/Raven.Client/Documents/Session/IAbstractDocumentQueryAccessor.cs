using Raven.Client.Documents.Queries.Vector;

namespace Raven.Client.Documents.Session;

internal interface IAbstractDocumentQueryAccessor
{
    void VectorSearch(IVectorEmbeddingFieldFactoryAccessor fieldFactoryAccessor, IVectorFieldValueFactoryAccessor fieldValueFactoryAccessor, float? minimumSimilarity, int? numberOfCandidates, bool isExact);
}
