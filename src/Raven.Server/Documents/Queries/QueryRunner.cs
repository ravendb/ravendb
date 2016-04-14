using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Primitives;

using Raven.Client.Data;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Queries.Dynamic;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Queries
{
    public class QueryRunner
    {
        private readonly IndexStore _indexStore;
        private readonly DocumentsStorage _documentsStorage;
        private readonly DocumentsOperationContext _documentsContext;

        public QueryRunner(IndexStore indexStore, DocumentsStorage documentsStorage, DocumentsOperationContext documentsContext)
        {
            _indexStore = indexStore;
            _documentsStorage = documentsStorage;
            _documentsContext = documentsContext;
        }

        public async Task<DocumentQueryResult> ExecuteQuery(string indexName, IndexQuery query, StringValues includes, long? existingResultEtag, OperationCancelToken token)
        {
            DocumentQueryResult result;

            if (indexName.StartsWith("dynamic/", StringComparison.OrdinalIgnoreCase) ||
                indexName.Equals("dynamic", StringComparison.OrdinalIgnoreCase))
            {
                var runner = new DynamicQueryRunner(_indexStore, _documentsStorage, _documentsContext, token);

                result = await runner.Execute(indexName, query, existingResultEtag).ConfigureAwait(false);
            }
            else
            {
                throw new InvalidOperationException("We don't support querying of static indexes for now");
            }

            if (result.NotModified == false && includes.Count > 0)
            {
                var includeDocs = new IncludeDocumentsCommand(_documentsStorage, _documentsContext, includes);
                includeDocs.Execute(result.Results, result.Includes);
            }

            return result;
        }

        public TermsQueryResult ExecuteGetTermsQuery(string indexName, string field, string fromValue, long? existingResultEtag, int pageSize, DocumentsOperationContext context, OperationCancelToken token)
        {
            var index = _indexStore.GetIndex(indexName);
            if (index == null)
                throw new InvalidOperationException("There is not index with name: " + indexName);

            var etag = index.GetIndexEtag();
            if (etag == existingResultEtag)
                return new TermsQueryResult { NotModified = true };

            return index.GetTerms(field, fromValue, pageSize, context, token);
        }
    }
}