using System;
using System.Linq;
using System.Linq.Expressions;
using System.Net.Http;
using System.Text;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations
{
    /// <summary>
    ///     A class for creating operations that delete documents by a given query
    /// </summary>
    /// <typeparam name="TEntity">The C# class that represents the documents that are being deleted</typeparam>
    /// <typeparam name="TIndexCreator">The index that will be used to run the query on documents of type <see cref="TEntity"/></typeparam>
    public sealed class DeleteByQueryOperation<TEntity, TIndexCreator> : DeleteByQueryOperation<TEntity>
        where TIndexCreator : AbstractIndexCreationTask, new()
    {
        /// <summary>
        ///     Returns an awaitable operation which deletes all documents that satisfy the provided <param name="expression">query expression</param>
        /// </summary>
        /// <param name="expression">The criteria for selecting documents to be deleted</param>
        /// <param name="options">Provides additional options for configuring the query execution when deleting documents</param>
        public DeleteByQueryOperation(Expression<Func<TEntity, bool>> expression, QueryOperationOptions options = null)
            : base(new TIndexCreator().IndexName, expression, options)
        {
        }
    }

    /// <inheritdoc cref="DeleteByQueryOperation{TEntity, TIndexCreator}"/>
    public class DeleteByQueryOperation<TEntity> : DeleteByQueryOperation
    {
        private readonly string _indexName;
        private readonly Expression<Func<TEntity, bool>> _expression;

        /// <summary>
        ///     Returns an awaitable operation which deletes all documents that satisfy the provided <param name="expression">query expression</param>
        /// </summary>
        /// <param name="expression">The criteria for selecting documents to be deleted</param>
        /// <param name="options">Provides additional options for configuring the query execution when deleting documents</param>
        public DeleteByQueryOperation(string indexName, Expression<Func<TEntity, bool>> expression, QueryOperationOptions options = null)
            : base(DummyQuery, options)
        {
            _indexName = indexName ?? throw new ArgumentNullException(nameof(indexName));
            _expression = expression ?? throw new ArgumentNullException(nameof(expression));
        }

        public override RavenCommand<OperationIdResult> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            if (_queryToDelete == DummyQuery)
            {
                using (var session = store.OpenSession(string.IsNullOrWhiteSpace(store.Database) ? "DummyDatabase" : store.Database))
                {
                    var query = session
                        .Query<TEntity>(_indexName)
                        .Where(_expression);

                    var inspector = (IRavenQueryInspector)query;

                    _queryToDelete = inspector.GetIndexQuery(isAsync: false);
                }
            }

            return base.GetCommand(store, conventions, context, cache);
        }
    }

    /// <inheritdoc cref="DeleteByQueryOperation{TEntity, TIndexCreator}"/>
    public class DeleteByQueryOperation : IOperation<OperationIdResult>
    {
        protected static IndexQuery DummyQuery = new IndexQuery();

        protected IndexQuery _queryToDelete;
        private readonly QueryOperationOptions _options;

        /// <summary>
        ///     Returns an awaitable operation which deletes all documents that satisfy the provided <param name="queryToDelete">query</param>
        /// </summary>
        /// <param name="queryToDelete">The criteria for selecting documents to be deleted</param>
        public DeleteByQueryOperation(string queryToDelete)
            : this(new IndexQuery { Query = queryToDelete })
        {
        }

        /// <inheritdoc cref="DeleteByQueryOperation(string)"/>
        /// <param name="options">Provides additional options for configuring the query execution when deleting documents</param>
        public DeleteByQueryOperation(IndexQuery queryToDelete, QueryOperationOptions options = null)
        {
            _queryToDelete = queryToDelete ?? throw new ArgumentNullException(nameof(queryToDelete));
            _options = options;
        }

        public virtual RavenCommand<OperationIdResult> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new DeleteByQueryCommand<Parameters>(conventions, _queryToDelete, _options);
        }

        internal sealed class DeleteByQueryCommand<T> : RavenCommand<OperationIdResult>
        {
            private readonly DocumentConventions _conventions;
            private readonly IndexQuery<T> _queryToDelete;
            private readonly long? _operationId;
            private readonly QueryOperationOptions _options;

            public DeleteByQueryCommand(DocumentConventions conventions, IndexQuery<T> queryToDelete, QueryOperationOptions options = null, long? operationId = null)
            {
                _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
                _queryToDelete = queryToDelete ?? throw new ArgumentNullException(nameof(queryToDelete));
                _operationId = operationId;
                _options = options ?? new QueryOperationOptions();
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
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

                if (_operationId.HasValue)
                {
                    path
                        .Append("&operationId=")
                        .Append(_operationId.Value);
                }

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Delete,
                    Content = new BlittableJsonContent(async stream =>
                    {
                        await using (var writer = new AsyncBlittableJsonTextWriter(ctx, stream))
                        {
                            writer.WriteIndexQuery(_conventions, ctx, _queryToDelete);
                        }
                    }, _conventions)
                };

                url = path.ToString();
                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.OperationIdResult(response);
            }

            public override bool IsReadRequest => false;
        }
    }
}
