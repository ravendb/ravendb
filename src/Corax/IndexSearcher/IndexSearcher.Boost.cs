using System;
using Corax.Queries;

namespace Corax;

public partial class IndexSearcher
{
    public BoostingMatch Boost(IQueryMatch match, float boostFactor)
    {
        return new BoostingMatch(this, match, boostFactor);
    }
}
