using System;

using Raven.Abstractions.Data;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Queries.Dynamic;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Queries
{
    public class QueryRunner
    {
        private readonly IndexStore _indexStore;
        private readonly DocumentsOperationContext _context;

        public QueryRunner(IndexStore indexStore, DocumentsOperationContext context)
        {
            _indexStore = indexStore;
            _context = context;
        }

        public DocumentQueryResult ExecuteQuery(string indexName, IndexQuery query)
        {
            if (indexName.StartsWith("dynamic/", StringComparison.OrdinalIgnoreCase) ||
                indexName.Equals("dynamic", StringComparison.OrdinalIgnoreCase))
            {
                var runner = new DynamicQueryRunner(_indexStore, _context);

                return runner.Execute(indexName, query);
            }
            else
            {
                throw new InvalidOperationException("We don't support querying of static indexes for now");
            }
        }
    }
}