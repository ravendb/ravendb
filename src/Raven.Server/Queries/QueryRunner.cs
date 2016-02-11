using System;
using Raven.Abstractions.Data;
using Raven.Server.Queries.Dynamic;

namespace Raven.Server.Queries
{
    public class QueryRunner
    {
        public QueryResult ExecuteQuery(string indexName, IndexQuery query)
        {

            if (indexName.StartsWith("dynamic/", StringComparison.OrdinalIgnoreCase) ||
                indexName.Equals("dynamic", StringComparison.OrdinalIgnoreCase))
            {
                var runner = new DynamicQueryRunner(indexName, query);

                return runner.Execute();
            }
            else
            {
                throw new InvalidOperationException("We don't support querying of static indexes for now");
            }
        }
    }
}