using System;
using System.Collections.Generic;
using System.Linq;

namespace Raven.Server.Indexing.Corax.Queries
{
    public class BooleanQuery : Query
    {
        private readonly Query[] _subQueries;

        public BooleanQuery(QueryOperator op, params Query[] subQueries)
        {
            _subQueries = subQueries;
            Op = op;
        }

        public QueryOperator Op { get; set; }

        protected override void Init()
        {
            foreach (var sub in _subQueries)
            {
                sub.Initialize(Index, Context, IndexEntries);
            }
        }

        public override QueryMatch[] Execute()
        {
            if (_subQueries.Length == 0)
                return Array.Empty<QueryMatch>();
            if(_subQueries.Length == 1)
                return _subQueries[0].Execute();
            //TODO: Optimize this code
            var dictionary = new Dictionary<long, QueryMatch>();
            for (int i = 0; i < _subQueries.Length; i++)
            {
                foreach (var queryMatch in _subQueries[i].Execute())
                {
                    QueryMatch prev;
                    if (dictionary.TryGetValue(queryMatch.DocumentId, out prev))
                    {
                        prev.Score += queryMatch.Score;
                        prev.Matches++;
                    }
                    else
                    {
                        dictionary[queryMatch.DocumentId] = queryMatch;
                    }
                }
            }
            if (Op == QueryOperator.Or)
                return dictionary.Values.ToArray();
            return dictionary.Values.Where(x => x.Matches == _subQueries.Length).ToArray();
        }

        public override string ToString()
        {
            return string.Join<Query>(" " + Op + " ", _subQueries);
        }
    }
}
