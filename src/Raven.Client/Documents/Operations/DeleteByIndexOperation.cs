using System;
using System.Linq;
using System.Linq.Expressions;
using System.Net.Http;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations
{
    public class DeleteByIndexOperation<TEntity, TIndexCreator> : DeleteByIndexOperation<TEntity>
        where TIndexCreator : AbstractIndexCreationTask, new()
    {
        public DeleteByIndexOperation(Expression<Func<TEntity, bool>> expression, QueryOperationOptions options = null)
            : base(new TIndexCreator().IndexName, expression, options)
        {
        }

        protected DeleteByIndexOperation(string indexName, IndexQuery queryToDelete, QueryOperationOptions options = null)
            : base(indexName, queryToDelete, options)
        {
        }
    }

    public class DeleteByIndexOperation<TEntity> : DeleteByIndexOperation
    {
        private readonly Expression<Func<TEntity, bool>> _expression;

        public DeleteByIndexOperation(string indexName, Expression<Func<TEntity, bool>> expression, QueryOperationOptions options = null)
            : base(indexName, DummyQuery, options)
        {
            _expression = expression ?? throw new ArgumentNullException(nameof(expression));
        }

        protected DeleteByIndexOperation(string indexName, IndexQuery queryToDelete, QueryOperationOptions options = null)
            : base(indexName, queryToDelete, options)
        {
        }

        public override RavenCommand<OperationIdResult> GetCommand(IDocumentStore store, JsonOperationContext context, HttpCache cache)
        {
            if (_queryToDelete == DummyQuery)
            {
                using (var session = store.OpenSession())
                {
                    var query = session.Query<TEntity>().Where(_expression);
                    _queryToDelete = new IndexQuery
                    {
                        Query = query.ToString()
                    };
                }
            }

            return base.GetCommand(store, context, cache);
        }
    }

    public class DeleteByIndexOperation : IOperation<OperationIdResult>
    {
        protected static IndexQuery DummyQuery = new IndexQuery();

        private readonly string _indexName;
        protected IndexQuery _queryToDelete;
        private readonly QueryOperationOptions _options;

        public DeleteByIndexOperation(string indexName, IndexQuery queryToDelete, QueryOperationOptions options = null)
        {
            _indexName = indexName ?? throw new ArgumentNullException(nameof(indexName));
            _queryToDelete = queryToDelete ?? throw new ArgumentNullException(nameof(queryToDelete));
            _options = options;
        }

        public virtual RavenCommand<OperationIdResult> GetCommand(IDocumentStore store, JsonOperationContext context, HttpCache cache)
        {
            return new DeleteByIndexCommand(_indexName, _queryToDelete, _options);
        }

        private class DeleteByIndexCommand : RavenCommand<OperationIdResult>
        {
            private readonly string _indexName;
            private readonly IndexQuery _queryToDelete;
            private readonly QueryOperationOptions _options;

            public DeleteByIndexCommand(string indexName, IndexQuery queryToDelete, QueryOperationOptions options = null)
            {
                if (queryToDelete == null)
                    throw new ArgumentNullException(nameof(queryToDelete));

                _indexName = indexName ?? throw new ArgumentNullException(nameof(indexName));
                _queryToDelete = queryToDelete;
                _options = options ?? new QueryOperationOptions();
            }

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                var u = $"{node.Url}/databases/{node.Database}";
                url = $"{_queryToDelete.GetIndexQueryUrl(u, _indexName, "queries")}&allowStale=" +
                      $"{_options.AllowStale}&maxOpsPerSec={_options.MaxOpsPerSecond}&details={_options.RetrieveDetails}";

                if (_options.StaleTimeout != null)
                    url += "&staleTimeout=" + _options.StaleTimeout;

                return new HttpRequestMessage
                {
                    Method = HttpMethods.Delete
                };
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.OperationIdResult(response);
            }

            public override bool IsReadRequest => false;
        }
    }
}