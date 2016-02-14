using System;
using System.Collections.Generic;
using System.Linq;

namespace Corax.Queries
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

		    //TODO: implement proper merge sort

            var results = new HashSet<QueryMatch>(_subQueries[0].Execute(), QueryMatchComparer.Instance);
		    for (int index = 1; index < _subQueries.Length; index++)
		    {
		        switch (Op)
		        {
		            case QueryOperator.And:
		                results.IntersectWith(_subQueries[index].Execute());
		                break;
                    case QueryOperator.Or:
                        results.UnionWith(_subQueries[index].Execute());
                        break;
                    default:
		                throw new InvalidOperationException("Invalid operation " + Op);
		        }
		    }
		    return results.ToArray();
		}

		public override string ToString()
		{
			return string.Join<Query>(" " + Op + " ", _subQueries);
		}
	}
}
