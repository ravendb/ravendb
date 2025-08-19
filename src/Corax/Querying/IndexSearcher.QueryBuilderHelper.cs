using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Matches.SpatialMatch;

namespace Corax.Querying;

public partial class IndexSearcher
{
    private static class QueryBuilderHelper
    {
        public enum QueryType
        {
            TermMatch,
            BinaryMatch,
            MultiTermMatch,
            AndNotMatch,
            BoostingMatch,
            SpatialMatchNoBoosting,
            SpatialMatchHasBoosting,
            MultiUnaryMatch,
            IQueryMatch
        }

        public static QueryType GetQueryType<T>(in T match)
        {
            var type = match.GetType();
            if (type == typeof(TermMatch))
                return QueryType.TermMatch;

            if (type == typeof(BinaryMatch))
                return QueryType.BinaryMatch;
            
            if (type == typeof(MultiTermMatch))
                return QueryType.MultiTermMatch;
            
            if (type == typeof(AndNotMatch))
                return QueryType.AndNotMatch;
            
            if (type == typeof(BoostingMatch))
                return QueryType.BoostingMatch;
            
            if (type == typeof(SpatialMatch<NoBoosting>))
                return QueryType.SpatialMatchNoBoosting;
            
            if (type == typeof(SpatialMatch<HasBoosting>))
                return QueryType.SpatialMatchHasBoosting;
            
            if (type == typeof(MultiUnaryMatch))
                return QueryType.MultiUnaryMatch;

            return QueryType.IQueryMatch;
        }
    }
}
