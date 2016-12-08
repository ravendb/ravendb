//-----------------------------------------------------------------------
// <copyright file="AsyncDocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
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
using Raven.NewClient.Client.Linq;

namespace Raven.NewClient.Client.Document.Async
{
    /// <summary>
    /// Implementation for async document session 
    /// </summary>
    public partial class AsyncDocumentSession : InMemoryDocumentSessionOperations, IAsyncDocumentSessionImpl, IAsyncAdvancedSessionOperations, IDocumentQueryGenerator
    {
        private readonly AsyncDocumentKeyGeneration asyncDocumentKeyGeneration;

        /// <summary>
        /// Initializes a new instance of the <see cref="AsyncDocumentSession"/> class.
        /// </summary>
        public AsyncDocumentSession(string dbName, DocumentStore documentStore, RequestExecuter requestExecuter, Guid id)
            : base(dbName, documentStore, requestExecuter, id)
        {
            GenerateDocumentKeysOnStore = false;
            asyncDocumentKeyGeneration = new AsyncDocumentKeyGeneration(this, DocumentsByEntity.TryGetValue, (key, entity, metadata) => key);
        }

        public Task<FacetedQueryResult[]> MultiFacetedSearchAsync(params FacetQuery[] queries)
        {
            throw new NotImplementedException();
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

        public IDictionary<string, string> GetMetadataForAsync<T>(T instance)
        {
            return GetMetadataFor(instance);
        }

        public async Task<Operation> DeleteByIndexAsync<T, TIndexCreator>(Expression<Func<T, bool>> expression) where TIndexCreator : AbstractIndexCreationTask, new()
        {
            var indexCreator = new TIndexCreator();
            return await DeleteByIndexAsync<T>(indexCreator.IndexName, expression).ConfigureAwait(false);
        }

        public async Task<Operation> DeleteByIndexAsync<T>(string indexName, Expression<Func<T, bool>> expression)
        {
            var query = Query<T>(indexName).Where(expression);
            var indexQuery = new IndexQuery()
            {
                Query = query.ToString()
            };
            var deleteByIndexOperation = new DeleteByIndexOperation(Context);
            var command = deleteByIndexOperation.CreateRequest(indexName, indexQuery,
                new QueryOperationOptions(), (DocumentStore)this.DocumentStore);

            if (command != null)
            {
                await RequestExecuter.ExecuteAsync(command, Context);
                return new Operation(command.Result.OperationId);
            }
            return null;
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
            asyncDocumentKeyGeneration.Add(entity);
        }

        protected override Task<string> GenerateKeyAsync(object entity)
        {
            return Conventions.GenerateDocumentKeyAsync(DatabaseName, entity);
        }

        public IAsyncEagerSessionOperations Eagerly { get; }

        public IAsyncLazySessionOperations Lazily { get; }

        /// <summary>
        /// Begins the async save changes operation
        /// </summary>
        /// <returns></returns>
        public async Task SaveChangesAsync(CancellationToken token = default(CancellationToken))
        {
            await asyncDocumentKeyGeneration.GenerateDocumentKeysForSaveChanges().WithCancellation(token).ConfigureAwait(false);

            var saveChangesOeration = new BatchOperation(this);

            var command = saveChangesOeration.CreateRequest();
            if (command != null)
            {
                await RequestExecuter.ExecuteAsync(command, Context, token);
                saveChangesOeration.SetResult(command.Result);
            }
        }
    }
}
