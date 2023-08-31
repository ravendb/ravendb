using Corax.Queries;
using Corax.Queries.Meta;

namespace Corax.IndexSearcher;

public partial class IndexSearcher
{
    public BoostingMatch Boost(IQueryMatch match, float boostFactor)
    {
        return new BoostingMatch(this, match, boostFactor);
    }
}
