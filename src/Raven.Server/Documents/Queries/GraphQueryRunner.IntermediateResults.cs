using System;
using System.Collections.Generic;

namespace Raven.Server.Documents.Queries
{
    public partial class GraphQueryRunner
    {
        public struct IntermediateResults// using struct because the size is 16 bytes only
        {
            private Dictionary<string, List<Match>> _anonymousMatchesByAlias;
            private Dictionary<string, Dictionary<string, Match>> _matchesByAlias;



            public void Add(Match match)
            {
                foreach (var alias in match.Aliases)
                {
                    Add(alias, match, match.GetSingleDocumentResult(alias));
                }
            }

            public void Add(string alias, Match match, Document instance)
            {
                //we have map/reduce result that has no id since it is not a document (results of map/reduce query)
                if (instance.Id == null) 
                {
                    _anonymousMatchesByAlias[alias].Add(match);
                }
                else
                {
                    _matchesByAlias[alias][instance.Id] = match;
                }
            }

            public bool TryGetByAlias(string alias, out Dictionary<string,Match> value)
            {
                return _matchesByAlias.TryGetValue(alias, out value);
            }

            public bool TryGetMatchesForAlias(string alias, out ICollection<Match> value)
            {
                if(_anonymousMatchesByAlias.TryGetValue(alias, out var anonymousAliases) && 
                    anonymousAliases.Count > 0)
                {
                    value = anonymousAliases;
                    return true;
                }
                if(_matchesByAlias.TryGetValue(alias, out var dic) == false)
                {
                    value = null;
                    return false;
                }
                value = dic.Values;
                return true;
            }


            public void EnsureExists(string alias)
            {
                if (_matchesByAlias == null)
                    _matchesByAlias = new Dictionary<string, Dictionary<string, Match>>(StringComparer.OrdinalIgnoreCase);
                if (_anonymousMatchesByAlias == null)
                    _anonymousMatchesByAlias = new Dictionary<string, List<Match>>(StringComparer.OrdinalIgnoreCase);
                if (_anonymousMatchesByAlias.TryGetValue(alias, out _) == false)
                    _anonymousMatchesByAlias[alias] = new List<Match>();
                if (_matchesByAlias.TryGetValue(alias, out _) == false)
                    _matchesByAlias[alias] =  new Dictionary<string, Match>(StringComparer.OrdinalIgnoreCase);
            }
        }
    }
}
