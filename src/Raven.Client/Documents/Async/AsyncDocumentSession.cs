//-----------------------------------------------------------------------
// <copyright file="AsyncDocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util;
using Raven.Client.Connection;
using Raven.Client.Connection.Async;
using Raven.Client.Document.SessionOperations;
using Raven.Client.Linq;
using Raven.Client.Indexes;
using Raven.Json.Linq;
using Raven.Client.Document.Batches;
using System.Diagnostics;
using System.Dynamic;
using Raven.Abstractions.Commands;
using Raven.Client.Data;
using Raven.Client.Data.Queries;
using Raven.Client.Document;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.SessionOperations;
using Raven.Client.Http;
using Sparrow.Json;
using LoadOperation = Raven.Client.Documents.SessionOperations.LoadOperation;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Async
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
        public AsyncDocumentSession(string dbName, DocumentStore documentStore, IAsyncDatabaseCommands asyncDatabaseCommands, RequestExecuter requestExecuter, Guid id)
            : base(dbName, documentStore, requestExecuter, id)
        {
            AsyncDatabaseCommands = asyncDatabaseCommands;
            GenerateDocumentKeysOnStore = false;
            asyncDocumentKeyGeneration = new AsyncDocumentKeyGeneration(this, DocumentsByEntity.TryGetValue, (key, entity, metadata) => key);
        }

        /// <summary>
        /// Gets the async database commands.
        /// </summary>
        /// <value>The async database commands.</value>
        public IAsyncDatabaseCommands AsyncDatabaseCommands { get; private set; }

        public Task<FacetedQueryResult[]> MultiFacetedSearchAsync(params FacetQuery[] queries)
        {
            throw new NotImplementedException();
        }

        public string GetDocumentUrl(object entity)
        {
            DocumentInfo value;
            if (DocumentsByEntity.TryGetValue(entity, out value) == false)
                throw new InvalidOperationException("Could not figure out identifier for transient instance");

            return AsyncDatabaseCommands.UrlFor(value.Id);
        }

        public async Task RefreshAsync<T>(T entity, CancellationToken token = default(CancellationToken))
        {
            DocumentInfo documentInfo;
            if (DocumentsByEntity.TryGetValue(entity, out documentInfo) == false)
                throw new InvalidOperationException("Cannot refresh a transient instance");
            IncrementRequestCount();

            //TODO - Efrat - Change when we have new DatabaseCommands.Get
            //TODO - Efrat - fix after pull
            var document = await TempAsyncDatabaseCommandGet(documentInfo);

            RefreshInternal(entity, document, documentInfo);
        }

        public Task<Operation> DeleteByIndexAsync<T, TIndexCreator>(Expression<Func<T, bool>> expression) where TIndexCreator : AbstractIndexCreationTask, new()
        {
            throw new NotImplementedException();
        }

        public Task<Operation> DeleteByIndexAsync<T>(string indexName, Expression<Func<T, bool>> expression)
        {
            throw new NotImplementedException();
        }

        public IAsyncAdvancedSessionOperations Advanced { get; }

        /// <summary>
        /// Get the json document by key from the store
        /// </summary>
        protected override DocumentInfo GetDocumentInfo(string documentId)
        {
            throw new NotSupportedException("Cannot get a document in a synchronous manner using async document session");
        }

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
            return Conventions.GenerateDocumentKeyAsync(DatabaseName, AsyncDatabaseCommands, entity);
        }

        private readonly List<object> _entitiesWithMetadataInstance = new List<object>();

        public async Task<IDictionary<string, string>> GetMetadataForAsync<T>(T instance)
        {
            var documentInfo = await GetDocumentInfo(instance).ConfigureAwait(false);

            if (documentInfo.MetadataInstance != null)
                return documentInfo.MetadataInstance;

            var metadataAsBlittable = documentInfo.Metadata;
            var metadata = new MetadataAsDictionary(metadataAsBlittable);
            _entitiesWithMetadataInstance.Add(documentInfo.Entity);
            documentInfo.MetadataInstance = metadata;
            return metadata;
        }

        private async Task<DocumentInfo> GetDocumentInfo<T>(T instance)
        {
            DocumentInfo value;
            string id;
            if (DocumentsByEntity.TryGetValue(instance, out value) ||
                (!GenerateEntityIdOnTheClient.TryGetIdFromInstance(instance, out id) &&
                 (!(instance is IDynamicMetaObjectProvider) ||
                  !GenerateEntityIdOnTheClient.TryGetIdFromDynamic(instance, out id)))) return value;
            AssertNoNonUniqueInstance(instance, id);
            var documentInfo = new DocumentInfo
            {
                Id = id,
                Entity = instance
            };
            await TempAsyncDatabaseCommandGet(documentInfo);
            return documentInfo;
        }

        private async Task<BlittableJsonReaderObject> TempAsyncDatabaseCommandGet(DocumentInfo documentInfo, CancellationToken token = default(CancellationToken))
        {
            var command = new GetDocumentCommand
            {
                Ids = new[] { documentInfo.Id }
            };
            await RequestExecuter.ExecuteAsync(command, Context, token);
            var document = (BlittableJsonReaderObject)command.Result.Results[0];
            if (document == null)
                throw new InvalidOperationException("Document '" + documentInfo.Id +
                                                    "' no longer exists and was probably deleted");

            object metadata;
            document.TryGetMember(Constants.Metadata.Key, out metadata);
            documentInfo.Metadata = metadata as BlittableJsonReaderObject;

            object etag;
            document.TryGetMember(Constants.Metadata.Etag, out etag);
            documentInfo.ETag = etag as long?;

            documentInfo.Document = document;
            return document;
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
