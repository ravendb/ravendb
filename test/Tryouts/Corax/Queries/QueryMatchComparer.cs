using System.Collections.Generic;

namespace Corax.Queries
{
	public class QueryMatchComparer : IEqualityComparer<QueryMatch>
	{
		public static readonly QueryMatchComparer Instance = new QueryMatchComparer();

		public bool Equals(QueryMatch x, QueryMatch y)
		{
			return x.DocumentId == y.DocumentId;
		}

		public int GetHashCode(QueryMatch match)
		{
			return match.DocumentId.GetHashCode();
		}
	}

    public class QueryMatchScoreSorter : IComparer<QueryMatch>
    {
        public static QueryMatchScoreSorter Instance = new QueryMatchScoreSorter();
        public int Compare(QueryMatch x, QueryMatch y)
        {
            var compareTo = y.Score.CompareTo(x.Score);
            if (compareTo == 0)
                return x.DocumentId.CompareTo(y.DocumentId);
            return compareTo;
        }
    }
}