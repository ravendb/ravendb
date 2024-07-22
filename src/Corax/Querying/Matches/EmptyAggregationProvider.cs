using System;
using System.Collections.Generic;
using Corax.Querying.Matches.Meta;

namespace Corax.Querying.Matches;

public struct EmptyAggregationProvider : IAggregationProvider
{
    public IDisposable AggregateByTerms(out List<string> terms, out Span<long> counts)
    {
        terms = null;
        counts = Span<long>.Empty;
        return null;
    }

    public long AggregateByRange()
    {
        return 0;
    }
}
