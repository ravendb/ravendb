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
                //we have map/reduce result that has no id since it is not a document
                //in such case we have no choice but to use map/reduce result json as key since we have no other way to generate a key
                //TODO : discuss with Oren & guys - perhaps there is a better way? (see http://issues.hibernatingrhinos.com/issue/RavenDB-12164)
                //perhaps we can generate for map/reduce query results some sort of unique ids? perhaps even a auto-increment number?
                if (instance.Id == null) 
                {
                    
                    MatchesByAlias[alias][instance.Data.ToString()] = match;
                }
                else
                {
                    MatchesByAlias[alias][instance.Id] = match;
                }
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
