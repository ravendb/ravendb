using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Session.Tokens;

namespace Raven.Client.Documents.Session
{
    public abstract partial class AbstractDocumentQuery<T, TSelf>
    {
        protected List<CounterIncludesToken> CounterIncludesTokens;

        protected void IncludeCounters(string alias, 
            Dictionary<string, (bool All, HashSet<string> Counters)> countersToIncludeByDocId)
        {
            if (countersToIncludeByDocId?.Count > 0 == false)
                return;

            CounterIncludesTokens = new List<CounterIncludesToken>();
            _includesAlias = alias;

            foreach (var kvp in countersToIncludeByDocId)
            {
                if (kvp.Value.All)
                {
                    CounterIncludesTokens.Add(CounterIncludesToken.All(kvp.Key));
                    continue;
                }


                if (kvp.Value.Counters?.Count > 0 == false)
                    continue;

                CounterIncludesTokens.Add(CounterIncludesToken.Create(
                    kvp.Key,
                    kvp.Value.Counters.Count == 1
                        ? AddQueryParameter(kvp.Value.Counters.First())
                        : AddQueryParameter(kvp.Value.Counters)));
            }
        }
    }
}
