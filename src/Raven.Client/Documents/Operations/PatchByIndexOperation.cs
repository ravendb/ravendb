using System;
using System.Linq;
using System.Linq.Expressions;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations
{
    public class PatchByIndexOperation<TEntity, TIndexCreator> : PatchByIndexOperation<TEntity>
        where TIndexCreator : AbstractIndexCreationTask, new()
    {
        public PatchByIndexOperation(Expression<Func<TEntity, bool>> expression, PatchRequest patch, QueryOperationOptions options = null)
            : base(new TIndexCreator().IndexName, expression, patch, options)
        {
        }
    }

    public class PatchByIndexOperation<TEntity> : PatchByIndexOperation
    {
        private readonly string _indexName;
        private readonly Expression<Func<TEntity, bool>> _expression;

        public PatchByIndexOperation(string indexName, Expression<Func<TEntity, bool>> expression, PatchRequest patch, QueryOperationOptions options = null)
            : base(DummyQuery, patch, options)
        {
            _indexName = indexName ?? throw new ArgumentNullException(nameof(indexName));
            _expression = expression ?? throw new ArgumentNullException(nameof(expression));
        }

        public override RavenCommand<OperationIdResult> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            if (_queryToUpdate == DummyQuery)
            {
                using (var session = store.OpenSession())
                {
                    var query = session.Query<TEntity>(_indexName).Where(_expression);
                    _queryToUpdate = new IndexQuery
                    {
                        Query = query.ToString()
                    };
                }
            }

            return base.GetCommand(store, conventions, context, cache);
        }
    }

    public class PatchByIndexOperation : IOperation<OperationIdResult>
    {
        protected static IndexQuery DummyQuery = new IndexQuery();

        protected IndexQuery _queryToUpdate;
        private readonly PatchRequest _patch;
        private readonly QueryOperationOptions _options;

        public PatchByIndexOperation(IndexQuery queryToUpdate, PatchRequest patch, QueryOperationOptions options = null)
        {
            _queryToUpdate = queryToUpdate ?? throw new ArgumentNullException(nameof(queryToUpdate));
            _patch = patch ?? throw new ArgumentNullException(nameof(patch));
            _options = options;
        }

        public virtual RavenCommand<OperationIdResult> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new PatchByIndexCommand(conventions, context, _queryToUpdate, _patch, _options);
        }

        private class PatchByIndexCommand : RavenCommand<OperationIdResult>
        {
            private readonly JsonOperationContext _context;
            private readonly DocumentConventions _conventions;
            private readonly IndexQuery _queryToUpdate;
            private readonly BlittableJsonReaderObject _patch;
            private readonly QueryOperationOptions _options;

            public PatchByIndexCommand(DocumentConventions conventions, JsonOperationContext context, IndexQuery queryToUpdate, PatchRequest patch, QueryOperationOptions options = null)
            {
                if (patch == null)
                    throw new ArgumentNullException(nameof(patch));

                _context = context ?? throw new ArgumentNullException(nameof(context));
                _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
                _queryToUpdate = queryToUpdate ?? throw new ArgumentNullException(nameof(queryToUpdate));
                _patch = EntityToBlittable.ConvertEntityToBlittable(patch, conventions, _context);
                _options = options ?? new QueryOperationOptions();
            }

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/queries{_queryToUpdate.GetQueryString(_conventions)}&allowStale={_options.AllowStale}&maxOpsPerSec={_options.MaxOpsPerSecond}&details={_options.RetrieveDetails}";

                if (_options.StaleTimeout != null)
                    url += "&staleTimeout=" + _options.StaleTimeout;

                var request = new HttpRequestMessage
                {
                    Method = HttpMethods.Patch,
                    Content = new BlittableJsonContent(stream =>
                    {
                        _context.Write(stream, _patch);
                    })
                };

                return request;
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