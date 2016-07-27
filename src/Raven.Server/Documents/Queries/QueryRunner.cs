using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using Microsoft.Extensions.Primitives;

using Raven.Client.Data;
using Raven.Client.Data.Indexes;
using Raven.Client.Data.Queries;
using Raven.Client.Util.RateLimiting;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Queries.Dynamic;
using Raven.Server.Documents.Queries.MoreLikeThis;
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

        public async Task<DocumentQueryResult> ExecuteQuery(string indexName, IndexQueryServerSide query, StringValues includes, long? existingResultEtag, OperationCancelToken token)
        {
            DocumentQueryResult result;

            if (indexName.StartsWith("dynamic/", StringComparison.OrdinalIgnoreCase) ||
                indexName.Equals("dynamic", StringComparison.OrdinalIgnoreCase))
            {
                var runner = new DynamicQueryRunner(_database.IndexStore, _database.TransformerStore, _database.DocumentsStorage, _documentsContext, token);

                result = await runner.Execute(indexName, query, existingResultEtag).ConfigureAwait(false);
            }
            else
            {
                var index = GetIndex(indexName);
                var etag = index.GetIndexEtag();
                if (etag == existingResultEtag)
                    return new DocumentQueryResult { NotModified = true };

                return await index.Query(query, _documentsContext, token);
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

        public MoreLikeThisQueryResultServerSide ExecuteMoreLikeThisQuery(string indexName, MoreLikeThisQueryServerSide query, DocumentsOperationContext context, long? existingResultEtag, OperationCancelToken token)
        {
            if (query == null)
                throw new ArgumentNullException(nameof(query));

            if (string.IsNullOrEmpty(query.DocumentId) && query.MapGroupFields.Count == 0)
                throw new InvalidOperationException("The document id or map group fields are mandatory");

            var index = GetIndex(indexName);

            var etag = index.GetIndexEtag();
            if (etag == existingResultEtag)
                return new MoreLikeThisQueryResultServerSide { NotModified = true };

            context.OpenReadTransaction();

            return index.MoreLikeThisQuery(query, context, token);
        }

        public List<DynamicQueryToIndexMatcher.Explanation> ExplainDynamicIndexSelection(string indexName, IndexQueryServerSide indexQuery)
        {
            if (string.IsNullOrWhiteSpace(indexName) || (indexName.StartsWith("dynamic/", StringComparison.OrdinalIgnoreCase) == false && indexName.Equals("dynamic", StringComparison.OrdinalIgnoreCase) == false))
                throw new InvalidOperationException("Explain can only work on dynamic indexes");

            var runner = new DynamicQueryRunner(_database.IndexStore, _database.TransformerStore, _database.DocumentsStorage, _documentsContext, OperationCancelToken.None);

            return runner.ExplainIndexSelection(indexName, indexQuery);
        }

        public Task ExecuteDeleteQuery(string indexName, IndexQueryServerSide query, QueryOperationOptions options, DocumentsOperationContext context, OperationCancelToken token)
        {
            return ExecuteOperation(indexName, query, options, context, key => _database.DocumentsStorage.Delete(context, key, null), token);
        }

        public Task ExecutePatchQuery(string indexName, IndexQueryServerSide query, QueryOperationOptions options, PatchRequest patch, DocumentsOperationContext context, OperationCancelToken token)
        {
            return ExecuteOperation(indexName, query, options, context, key => _database.Patch.Apply(context, key, null, patch, null), token);
        }

        private async Task ExecuteOperation(string indexName, IndexQueryServerSide query, QueryOperationOptions options, DocumentsOperationContext context, Action<string> action, OperationCancelToken token)
        {
            var index = GetIndex(indexName);

            if (index.Type.IsMapReduce())
                throw new InvalidOperationException("Cannot execute Delete operation on Map-Reduce indexes.");

            query = ConvertToOperationQuery(query, options);

            const int BatchSize = 1024;

            RavenTransaction tx = null;
            var operations = 0;
            var results = await index.Query(query, context, token).ConfigureAwait(false);
            context.Reset();

            if (options.AllowStale == false && results.IsStale)
                throw new InvalidOperationException("Cannot perform delete operation. Query is stale.");

            using (var rateGate = options.MaxOpsPerSecond.HasValue ? new RateGate(options.MaxOpsPerSecond.Value, TimeSpan.FromSeconds(1)) : null)
            {
                foreach (var document in results.Results)
                {
                    if (rateGate != null && rateGate.WaitToProceed(0) == false)
                    {
                        using (tx)
                        {
                            tx?.Commit();
                        }

                        tx = null;

                        rateGate.WaitToProceed();
                    }

                    if (tx == null)
                    {
                        operations = 0;
                        tx = context.OpenWriteTransaction();
                    }

                    action(document.Key);
                    operations++;

                    if (operations < BatchSize)
                        continue;

                    using (tx)
                    {
                        tx.Commit();
                    }

                    tx = null;
                }
            }

            using (tx)
            {
                tx?.Commit();
            }
        }

        private static IndexQueryServerSide ConvertToOperationQuery(IndexQueryServerSide query, QueryOperationOptions options)
        {
            return new IndexQueryServerSide
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