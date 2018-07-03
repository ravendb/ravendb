using System.Collections.Generic;
using Raven.Client.Documents.Session.Tokens;

namespace Raven.Client.Documents.Session
{
    public abstract partial class AbstractDocumentQuery<T, TSelf>
    {
        protected CounterIncludesToken CounterIncludesToken;

        protected void IncludeCounters(bool includeAll, HashSet<string> counters)
        {           
            if (includeAll == false)
            {
                if (counters?.Count > 0 == false)
                    return;

                CounterIncludesToken = CounterIncludesToken.Create(counters, QueryParameters);
                return;
            }

            CounterIncludesToken = CounterIncludesToken.Create(null, null);
        }
    }
}
