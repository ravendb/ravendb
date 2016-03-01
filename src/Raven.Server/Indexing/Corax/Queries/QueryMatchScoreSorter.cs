using System.Collections.Generic;

namespace Raven.Server.Indexing.Corax.Queries
{
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