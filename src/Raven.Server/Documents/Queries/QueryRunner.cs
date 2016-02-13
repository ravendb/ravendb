using System;

using Raven.Abstractions.Data;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Queries.Dynamic;

namespace Raven.Server.Documents.Queries
{
    public class QueryRunner
    {
        private readonly IndexStore _indexStore;

        public QueryRunner(IndexStore indexStore)
        {
            _indexStore = indexStore;
        }

        public QueryResult ExecuteQuery(string indexName, IndexQuery query)
        {
            if (indexName.StartsWith("dynamic/", StringComparison.OrdinalIgnoreCase) ||
                indexName.Equals("dynamic", StringComparison.OrdinalIgnoreCase))
            {
                var runner = new DynamicQueryRunner(_indexStore);

                return runner.Execute(indexName, query);
            }
            else
            {
                throw new InvalidOperationException("We don't support querying of static indexes for now");
            }
        }
    }
}