using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Corax.Queries
{
	public class BooleanQuery : Query
	{
		private readonly Query[] _clasues;

		public BooleanQuery(QueryOperator op, params Query[] clasues)
		{
			_clasues = clasues;
			Op = op;
		}

		public QueryOperator Op { get; set; }

		protected override void Init()
		{
			foreach (var clasue in _clasues)
			{
				clasue.Initialize(Index, Transaction, Score);
			}
			Boost = _clasues.Sum(x => x.Boost);
		}

		public override IEnumerable<QueryMatch> Execute()
		{
			if (_clasues.Length == 0)
				return Enumerable.Empty<QueryMatch>();
			var result = _clasues[0].Execute();
			for (int i = 1; i < _clasues.Length; i++)
			{
				var temp = _clasues[i].Execute();
				switch (Op)
				{
					case QueryOperator.And:
						result = result.Intersect(temp	, QueryMatchComparer.Instance);
						break;
					case QueryOperator.Or:
						result = result.Union(temp, QueryMatchComparer.Instance);
						break;
					default:
						throw new ArgumentOutOfRangeException("Cannot understand " + Op);
				}	
			}
			return result;
		}

		public override string ToString()
		{
			return string.Join<Query>(" " + Op + " ", _clasues);
		}
	}
}
