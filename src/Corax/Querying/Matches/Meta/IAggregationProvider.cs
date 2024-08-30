using System;
using System.Collections.Generic;

namespace Corax.Querying.Matches.Meta;

public interface IAggregationProvider
{
    public IDisposable AggregateByTerms(out List<string> terms, out Span<long> counts);
    public long AggregateByRange();

}
