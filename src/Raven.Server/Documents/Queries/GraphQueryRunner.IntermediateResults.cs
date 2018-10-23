using System;
using System.Collections.Generic;

namespace Raven.Server.Documents.Queries
{
    public partial class GraphQueryRunner
    {
        public struct IntermediateResults// using struct because we have a single field 
        {
            private Dictionary<string, Dictionary<string, Match>> _matchesByAlias;
            private Dictionary<string, Dictionary<string, Match>> MatchesByAlias => 
                _matchesByAlias ??( _matchesByAlias = new Dictionary<string, Dictionary<string, Match>>(StringComparer.OrdinalIgnoreCase));

            public void Add(Match match)
            {
                foreach (var alias in match.Aliases)
                {
                    Add(alias, match, match.Get(alias));
                }
            }

            public void Add(string alias, Match match, Document instance)
            {
                //TODO: need to handle map/reduce results?
                MatchesByAlias[alias][instance.Id] = match;
            }

            public bool TryGetByAlias(string alias, out Dictionary<string,Match> value)
            {
                return MatchesByAlias.TryGetValue(alias, out value);
            }

            public void EnsureExists(string alias)
            {
                if (MatchesByAlias.TryGetValue(alias, out _) == false)
                    MatchesByAlias[alias] =  new Dictionary<string, Match>(StringComparer.OrdinalIgnoreCase);
            }
        }
    }
}
