using System.Collections;
using System.Collections.Generic;

namespace Raven.Studio.Models
{
    public class QueryState
    {
        public QueryState()
        {
            Query = string.Empty;
            SortOptions = new List<string>();
        }

        public string Query { get; set; }

        public IList<string> SortOptions { get; private set; }
    }
}