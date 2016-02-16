using System.Collections.Generic;

namespace Raven.Server.Indexing.Corax.Queries
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
}