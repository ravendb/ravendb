using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;

namespace Corax.Querying;

public partial class IndexSearcher
{
    public BoostingMatch Boost(IQueryMatch match, float boostFactor)
    {
        return new BoostingMatch(this, match, boostFactor);
    }
}
