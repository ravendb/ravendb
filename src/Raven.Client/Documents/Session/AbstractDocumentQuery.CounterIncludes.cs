using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Session.Tokens;

namespace Raven.Client.Documents.Session
{
    public abstract partial class AbstractDocumentQuery<T, TSelf>
    {
        protected CounterIncludesToken CounterIncludesToken;

        protected void IncludeCounters(bool includeAll, HashSet<string> counters)
        {
            if (includeAll)
            {
                CounterIncludesToken = CounterIncludesToken.All();
                return;
            }


            if (counters?.Count > 0 == false)
                return;

            CounterIncludesToken = CounterIncludesToken.Create(
                counters.Count == 1
                    ? AddQueryParameter(counters.First())
                    : AddQueryParameter(counters));
        }
    }
}
