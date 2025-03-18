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
}
