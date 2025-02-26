using Corax.Mappings;
using Corax.Querying.Matches;
using Corax.Utils;

namespace Corax.Querying;

public partial class IndexSearcher
{
    public VectorSearchMatch VectorSearch(in FieldMetadata metadata, in VectorValue vectorValue, float minimumMatch, in int numberOfCandidates, bool isExact, bool isSingleVectorSearch)
    {
        return new VectorSearchMatch(this, metadata, vectorValue, minimumMatch, numberOfCandidates, isExact, isSingleVectorSearch);
    }

    public MultiVectorSearchMatch MultiVectorSearch(in FieldMetadata metadata, in VectorValue[] vectorValues, float minimumMatch, in int numberOfCandidates, bool isExact,
        bool isSingleVectorSearch) => new MultiVectorSearchMatch(this, metadata, vectorValues, minimumMatch, numberOfCandidates, isExact, isSingleVectorSearch);
}
