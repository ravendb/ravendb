using System.Collections.Generic;
using Raven.Client.Silverlight.MissingFromSilverlight;
using System.Linq;

namespace Raven.Studio.Features.Query
{
    public class QueryState
    {
        public QueryState(string indexName, string query, IEnumerable<string> sortOptions)
        {
            IndexName = indexName;
            Query = query;
            SortOptions = sortOptions.ToList();
        }

        public string IndexName { get; private set; }

        public string Query { get; private set; }

        public IList<string> SortOptions { get; private set; }

        public string GetHash()
        {
            return MD5Core.GetHashString(IndexName + Query);
        }
    }
}