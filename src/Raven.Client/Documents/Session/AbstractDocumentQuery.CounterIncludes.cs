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

            if (alias != null)           
            {
                FromAlias = FromToken.Alias ?? alias;
            }

            foreach (var kvp in countersToIncludeByDocId)
            {
                var path = kvp.Key;
                if (alias != null)
                {
                    path = path == string.Empty 
                        ? FromAlias
                        : $"{FromAlias}.{path}";
                }

                if (kvp.Value.All)
                {
                    CounterIncludesTokens.Add(CounterIncludesToken.All(path));
                    return;
                }


                if (kvp.Value.Counters?.Count > 0 == false)
                    continue;

                CounterIncludesTokens.Add(CounterIncludesToken.Create(
                    path,
                    kvp.Value.Counters.Count == 1
                        ? AddQueryParameter(kvp.Value.Counters.First())
                        : AddQueryParameter(kvp.Value.Counters)));
            }
        }
    }
}
