using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using Microsoft.Extensions.Primitives;

using Raven.Client.Data;
using Raven.Client.Data.Indexes;
using Raven.Client.Util.RateLimiting;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Queries.Dynamic;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

using PatchRequest = Raven.Server.Documents.Patch.PatchRequest;

namespace Raven.Server.Documents.Queries
{
    public class QueryRunner
    {
        private readonly DocumentDatabase _database;

        private readonly DocumentsOperationContext _documentsContext;

        public QueryRunner(DocumentDatabase database, DocumentsOperationContext documentsContext)
        {
            _database = database;
            _documentsContext = documentsContext;
        }

        public async Task<DocumentQueryResult> ExecuteQuery(string indexName, IndexQuery query, StringValues includes, long? existingResultEtag, OperationCancelToken token)
        {
            DocumentQueryResult result;

            if (indexName.StartsWith("dynamic/", StringComparison.OrdinalIgnoreCase) ||
                indexName.Equals("dynamic", StringComparison.OrdinalIgnoreCase))
            {
                var runner = new DynamicQueryRunner(_database.IndexStore, _database.DocumentsStorage, _documentsContext, token);

                result = await runner.Execute(indexName, query, existingResultEtag).ConfigureAwait(false);
            }
            else
            {
                throw new InvalidOperationException("We don't support querying of static indexes for now");
            }

            if (result.NotModified == false && includes.Count > 0)
            {
                var includeDocs = new IncludeDocumentsCommand(_database.DocumentsStorage, _documentsContext, includes);
                includeDocs.Execute(result.Results, result.Includes);
            }

            return result;
        }

        public TermsQueryResult ExecuteGetTermsQuery(string indexName, string field, string fromValue, long? existingResultEtag, int pageSize, DocumentsOperationContext context, OperationCancelToken token)
        {
            var index = GetIndex(indexName);

            var etag = index.GetIndexEtag();
            if (etag == existingResultEtag)
                return new TermsQueryResult { NotModified = true };

            return index.GetTerms(field, fromValue, pageSize, context, token);
        }

        public Task ExecuteDeleteQuery(string indexName, IndexQuery query, QueryOperationOptions options, DocumentsOperationContext context, OperationCancelToken token)
        {
            return ExecuteOperation(indexName, query, options, context, key => _database.DocumentsStorage.Delete(context, key, null), token);
        }

        public Task ExecutePatchQuery(string indexName, IndexQuery query, QueryOperationOptions options, PatchRequest patch, DocumentsOperationContext context, OperationCancelToken token)
        {
            return ExecuteOperation(indexName, query, options, context, key => _database.Patch.Apply(context, key, null, patch, null), token);
        }

        private async Task ExecuteOperation(string indexName, IndexQuery query, QueryOperationOptions options, DocumentsOperationContext context, Action<string> action, OperationCancelToken token)
        {
            var index = GetIndex(indexName);

            if (index.Type == IndexType.AutoMapReduce || index.Type == IndexType.MapReduce)
                throw new InvalidOperationException("Cannot execute Delete operation on Map-Reduce indexes.");

            query = ConvertToOperationQuery(query, options);

            const int BatchSize = 1024;

            RavenTransaction tx = null;
            var operations = 0;
            var results = await index.Query(query, context, token).ConfigureAwait(false);
            context.Reset();

            if (options.AllowStale == false && results.IsStale)
                throw new InvalidOperationException("Cannot perform delete operation. Query is stale.");

            IEnumerable<Document> documents = results.Results;
            if (options.MaxOpsPerSecond.HasValue)
                documents = documents.LimitRate(options.MaxOpsPerSecond.Value, TimeSpan.FromSeconds(1));

            foreach (var document in documents)
            {
                if (tx == null)
                {
                    operations = 0;
                    tx = context.OpenWriteTransaction();
                }

                action(document.Key);
                operations++;

                if (operations < BatchSize)
                    continue;

                tx.Commit();
                tx = null;
            }

            tx?.Commit();
        }

        private static IndexQuery ConvertToOperationQuery(IndexQuery query, QueryOperationOptions options)
        {
            return new IndexQuery
            {
                Query = query.Query,
                Start = query.Start,
                WaitForNonStaleResultsTimeout = options.StaleTimeout,
                PageSize = int.MaxValue,
                SortedFields = query.SortedFields,
                HighlighterPreTags = query.HighlighterPreTags,
                HighlighterPostTags = query.HighlighterPostTags,
                HighlightedFields = query.HighlightedFields,
                HighlighterKeyName = query.HighlighterKeyName,
                TransformerParameters = query.TransformerParameters,
                Transformer = query.Transformer
            };
        }

        private Index GetIndex(string indexName)
        {
            var index = _database.IndexStore.GetIndex(indexName);
            if (index == null)
                throw new InvalidOperationException("There is not index with name: " + indexName);

            return index;
        }
    }
}