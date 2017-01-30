//-----------------------------------------------------------------------
// <copyright file="AsyncDocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Raven.NewClient.Abstractions.Extensions;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Connection;

using Raven.NewClient.Client.Indexes;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Data.Queries;
using Raven.NewClient.Client.Document.Batches;
using Raven.NewClient.Client.Http;
using Raven.NewClient.Client.Json;
using Raven.NewClient.Client.Linq;
using Raven.NewClient.Operations;
using Raven.NewClient.Operations.Databases.Documents;
using Sparrow.Json;

namespace Raven.NewClient.Client.Document.Async
{
    /// <summary>
    /// Implementation for async document session 
    /// </summary>
    public partial class AsyncDocumentSession : InMemoryDocumentSessionOperations, IAsyncDocumentSessionImpl, IAsyncAdvancedSessionOperations, IDocumentQueryGenerator
    {
        private AsyncDocumentKeyGeneration _asyncDocumentKeyGeneration;
        private OperationExecuter _operations;

        /// <summary>
        /// Initializes a new instance of the <see cref="AsyncDocumentSession"/> class.
        /// </summary>
        public AsyncDocumentSession(string dbName, DocumentStore documentStore, RequestExecuter requestExecuter, Guid id)
            : base(dbName, documentStore, requestExecuter, id)
        {
            GenerateDocumentKeysOnStore = false;
        }

        public async Task<FacetedQueryResult[]> MultiFacetedSearchAsync(params FacetQuery[] queries)
        {
            IncrementRequestCount();
            var requests = new List<GetRequest>();
            var results = new List<FacetedQueryResult>();
            foreach (var q in queries)
            {
                var method = q.CalculateHttpMethod();
                requests.Add(new GetRequest
                {
                    Url = "/queries/" + q.IndexName,
                    Query = "?" + q.GetQueryString(method),
                    Method = method.Method,
                    Content = method == HttpMethod.Post ? q.GetFacetsAsJson() : null
                });
            }
            var multiGetOperation = new MultiGetOperation(this);
            var command = multiGetOperation.CreateRequest(requests);
            await RequestExecuter.ExecuteAsync(command, Context).ConfigureAwait(false);
            foreach (var result in command.Result)
            {
                results.Add(JsonDeserializationClient.FacetedQueryResult((BlittableJsonReaderObject)result.Result));
            }
            return results.ToArray();
        }

        public string GetDocumentUrl(object entity)
        {
            throw new NotImplementedException();
        }

        public async Task RefreshAsync<T>(T entity, CancellationToken token = default(CancellationToken))
        {
            DocumentInfo documentInfo;
            if (DocumentsByEntity.TryGetValue(entity, out documentInfo) == false)
                throw new InvalidOperationException("Cannot refresh a transient instance");
            IncrementRequestCount();

            var command = new GetDocumentCommand
            {
                Ids = new[] { documentInfo.Id },
                Context = this.Context
            };
            await RequestExecuter.ExecuteAsync(command, Context, token);

            RefreshInternal(entity, command, documentInfo);
        }

        public async Task<Operation> DeleteByIndexAsync<T, TIndexCreator>(Expression<Func<T, bool>> expression) where TIndexCreator : AbstractIndexCreationTask, new()
        {
            var indexCreator = new TIndexCreator();
            return await DeleteByIndexAsync<T>(indexCreator.IndexName, expression).ConfigureAwait(false);
        }

        public async Task<Operation> DeleteByIndexAsync<T>(string indexName, Expression<Func<T, bool>> expression)
        {
            var query = Query<T>(indexName).Where(expression);
            var indexQuery = new IndexQuery(Conventions)
            {
                Query = query.ToString()
            };
            if (_operations == null)
                _operations = new OperationExecuter(_documentStore, _requestExecuter, Context);

            return await _operations.SendAsync(new DeleteByIndexOperation(indexName, indexQuery));
        }

        /// <summary>
        /// Get the accessor for advanced operations
        /// </summary>
        /// <remarks>
        /// Those operations are rarely needed, and have been moved to a separate
        /// property to avoid cluttering the API
        /// </remarks>
        public IAsyncAdvancedSessionOperations Advanced => this;

        protected override string GenerateKey(object entity)
        {
            throw new NotSupportedException("Async session cannot generate keys synchronously");
        }

        protected override void RememberEntityForDocumentKeyGeneration(object entity)
        {
            EnsureAsyncDocumentKeyGeneration();
            _asyncDocumentKeyGeneration.Add(entity);
        }

        private void EnsureAsyncDocumentKeyGeneration()
        {
            if (_asyncDocumentKeyGeneration != null) return;
            _asyncDocumentKeyGeneration = new AsyncDocumentKeyGeneration(this, DocumentsByEntity.TryGetValue,
                (key, entity, metadata) => key);
        }

        protected override Task<string> GenerateKeyAsync(object entity)
        {
            return Conventions.GenerateDocumentKeyAsync(DatabaseName, entity);
        }

        public IAsyncEagerSessionOperations Eagerly => this;

        public IAsyncLazySessionOperations Lazily => this;

        /// <summary>
        /// Begins the async save changes operation
        /// </summary>
        /// <returns></returns>
        public async Task SaveChangesAsync(CancellationToken token = default(CancellationToken))
        {
            if (_asyncDocumentKeyGeneration != null)
            {
                await _asyncDocumentKeyGeneration.GenerateDocumentKeysForSaveChanges().WithCancellation(token).ConfigureAwait(false);
            }

            var saveChangesOperation = new BatchOperation(this);

            using (var command = saveChangesOperation.CreateRequest())
            {
                await RequestExecuter.ExecuteAsync(command, Context, token);
                saveChangesOperation.SetResult(command.Result);
            }
        }
    }
}
