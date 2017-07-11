using System;
using System.Linq;
using System.Linq.Expressions;
using System.Net.Http;
using System.Text;
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
            private readonly BlittableJsonReaderObject _queryToUpdate;
            private readonly BlittableJsonReaderObject _patch;
            private readonly QueryOperationOptions _options;

            public PatchByIndexCommand(DocumentConventions conventions, JsonOperationContext context, IndexQuery queryToUpdate, PatchRequest patch, QueryOperationOptions options = null)
            {
                if (queryToUpdate == null)
                    throw new ArgumentNullException(nameof(queryToUpdate));
                if (patch == null)
                    throw new ArgumentNullException(nameof(patch));

                _context = context ?? throw new ArgumentNullException(nameof(context));
                _queryToUpdate = EntityToBlittable.ConvertEntityToBlittable(queryToUpdate, conventions, _context);
                _patch = EntityToBlittable.ConvertEntityToBlittable(patch, conventions, _context);
                _options = options ?? new QueryOperationOptions();
            }

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                var path = new StringBuilder(node.Url)
                    .Append("/databases/")
                    .Append(node.Database)
                    .Append("/queries")
                    .Append("?allowStale=")
                    .Append(_options.AllowStale)
                    .Append("&maxOpsPerSec=")
                    .Append(_options.MaxOpsPerSecond)
                    .Append("&details=")
                    .Append(_options.RetrieveDetails);

                if (_options.StaleTimeout != null)
                {
                    path
                        .Append("&staleTimeout=")
                        .Append(_options.StaleTimeout.Value);
                }

                var request = new HttpRequestMessage
                {
                    Method = HttpMethods.Patch,
                    Content = new BlittableJsonContent(stream =>
                        {
                            using (var writer = new BlittableJsonTextWriter(_context, stream))
                            {
                                writer.WriteStartObject();

                                writer.WritePropertyName("Query");
                                writer.WriteObject(_queryToUpdate);
                                writer.WriteComma();

                                writer.WritePropertyName("Patch");
                                writer.WriteObject(_patch);

                                writer.WriteEndObject();
                            }
                        }
                    )
                };

                url = path.ToString();
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