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

namespace Raven.Client.Documents.Async
{
    /// <summary>
    /// Implementation for async document session 
    /// </summary>
    public class AsyncDocumentSession : InMemoryDocumentSessionOperations, IDocumentQueryGenerator, IAdvancedDocumentSessionOperations
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

        public string GetDocumentUrl(object entity)
        {
            DocumentInfo value;
            if (DocumentsByEntity.TryGetValue(entity, out value) == false)
                throw new InvalidOperationException("Could not figure out identifier for transient instance");

            return AsyncDatabaseCommands.UrlFor(value.Id);
        }

        internal Lazy<Task<T>> AddLazyOperation<T>(ILazyOperation operation, Action<T> onEval, CancellationToken token = default(CancellationToken))
        {
            pendingLazyOperations.Add(operation);
            var lazyValue = new Lazy<Task<T>>(() =>
                ExecuteAllPendingLazyOperationsAsync(token)
                .ContinueWith(t =>
            {
                if (t.Exception != null)
                    throw new InvalidOperationException("Could not perform add lazy operation", t.Exception);

                return GetOperationResult<T>(operation.Result);
            }, token));

            if (onEval != null)
                onEvaluateLazy[operation] = theResult => onEval(GetOperationResult<T>(theResult));

            return lazyValue;
        }

        internal Lazy<Task<int>> AddLazyCountOperation(ILazyOperation operation, CancellationToken token = default(CancellationToken))
        {
            pendingLazyOperations.Add(operation);
            var lazyValue = new Lazy<Task<int>>(() => ExecuteAllPendingLazyOperationsAsync(token)
                .ContinueWith(t =>
                {
                    if (t.Exception != null)
                        throw new InvalidOperationException("Could not perform lazy count", t.Exception);
                    return operation.QueryResult.TotalResults;
                }));

            return lazyValue;
        }
        public async Task<ResponseTimeInformation> ExecuteAllPendingLazyOperationsAsync(CancellationToken token = default(CancellationToken))
        {
            if (pendingLazyOperations.Count == 0)
                return new ResponseTimeInformation();

            try
            {
                var sw = Stopwatch.StartNew();

                IncrementRequestCount();

                var responseTimeDuration = new ResponseTimeInformation();

                while (await ExecuteLazyOperationsSingleStep(responseTimeDuration).WithCancellation(token).ConfigureAwait(false))
                {
                    await Task.Delay(100).WithCancellation(token).ConfigureAwait(false);
                }

                responseTimeDuration.ComputeServerTotal();


                foreach (var pendingLazyOperation in pendingLazyOperations)
                {
                    Action<object> value;
                    if (onEvaluateLazy.TryGetValue(pendingLazyOperation, out value))
                        value(pendingLazyOperation.Result);
                }
                responseTimeDuration.TotalClientDuration = sw.Elapsed;
                return responseTimeDuration;
            }
            finally
            {
                pendingLazyOperations.Clear();
            }
        }

        private async Task<bool> ExecuteLazyOperationsSingleStep(ResponseTimeInformation responseTimeInformation)
        {
            var disposables = pendingLazyOperations.Select(x => x.EnterContext()).Where(x => x != null).ToList();
            try
            {
                var requests = pendingLazyOperations.Select(x => x.CreateRequest()).ToArray();
                var responses = await AsyncDatabaseCommands.MultiGetAsync(requests).ConfigureAwait(false);

                for (int i = 0; i < pendingLazyOperations.Count; i++)
                {
                    long totalTime;
                    long.TryParse(responses[i].Headers[Constants.Headers.RequestTime], out totalTime);

                    responseTimeInformation.DurationBreakdown.Add(new ResponseTimeItem
                    {
                        Url = requests[i].UrlAndQuery,
                        Duration = TimeSpan.FromMilliseconds(totalTime)
                    });
                    if (responses[i].RequestHasErrors())
                    {
                        throw new InvalidOperationException("Got an error from server, status code: " + responses[i].Status +
                                                            Environment.NewLine + responses[i].Result);
                    }
                    pendingLazyOperations[i].HandleResponse(responses[i]);
                    if (pendingLazyOperations[i].RequiresRetry)
                    {
                        return true;
                    }
                }
                return false;
            }
            finally
            {
                foreach (var disposable in disposables)
                {
                    disposable.Dispose();
                }
            }
        }
        
        /// <summary>
        /// Begins the async load operation, with the specified id after applying
        /// conventions on the provided id to get the real document id.
        /// </summary>
        /// <remarks>
        /// This method allows you to call:
        /// LoadAsync{Post}(1)
        /// And that call will internally be translated to 
        /// LoadAsync{Post}("posts/1");
        /// 
        /// Or whatever your conventions specify.
        /// </remarks>
        public Task<T> LoadAsync<T>(ValueType id, CancellationToken token = default(CancellationToken))
        {
            var documentKey = Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(T), false);
            return LoadAsync<T>(documentKey, token);
        }

        /// <summary>
        /// Begins the async load operation, with the specified ids after applying
        /// conventions on the provided ids to get the real document ids.
        /// </summary>
        /// <remarks>
        /// This method allows you to call:
        /// LoadAsync{Post}(1,2,3)
        /// And that call will internally be translated to 
        /// LoadAsync{Post}("posts/1","posts/2","posts/3");
        /// 
        /// Or whatever your conventions specify.
        /// </remarks>
        public Task<T[]> LoadAsync<T>(CancellationToken token = default(CancellationToken), params ValueType[] ids)
        {
            var documentKeys = ids.Select(id => Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(T), false));
            return LoadAsync<T>(documentKeys, token);
        }

        /// <summary>
        /// Begins the async load operation, with the specified ids after applying
        /// conventions on the provided ids to get the real document ids.
        /// </summary>
        /// <remarks>
        /// This method allows you to call:
        /// LoadAsync{Post}(new List&lt;int&gt;(){1,2,3})
        /// And that call will internally be translated to 
        /// LoadAsync{Post}("posts/1","posts/2","posts/3");
        /// 
        /// Or whatever your conventions specify.
        /// </remarks>
        public Task<T[]> LoadAsync<T>(IEnumerable<ValueType> ids)
        {
            return LoadAsync<T>(ids, new CancellationToken());
        }

        /// <summary>
        /// Begins the async load operation, with the specified ids after applying
        /// conventions on the provided ids to get the real document ids.
        /// </summary>
        /// <remarks>
        /// This method allows you to call:
        /// LoadAsync{Post}(new List&lt;int&gt;(){1,2,3})
        /// And that call will internally be translated to 
        /// LoadAsync{Post}("posts/1","posts/2","posts/3");
        /// 
        /// Or whatever your conventions specify.
        /// </remarks>
        public Task<T[]> LoadAsync<T>(IEnumerable<ValueType> ids, CancellationToken token = default(CancellationToken))
        {
            var documentKeys = ids.Select(id => Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(T), false));
            return LoadAsync<T>(documentKeys, token);
        }

        /// <summary>
        /// Begins the async load operation
        /// </summary>
        /// <param name="id">The id.</param>
        /// <param name="token">The canecllation token.</param>
        /// <returns></returns>
        public async Task<T> LoadAsync<T>(string id, CancellationToken token = default(CancellationToken))
        {
            var loadOeration = new LoadOperation(this);
            loadOeration.ById(id);

            var command = loadOeration.CreateRequest();
            if (command != null)
            {
                await RequestExecuter.ExecuteAsync(command, Context);
                loadOeration.SetResult(command.Result);
            }

            return loadOeration.GetDocument<T>();
        }

        public async Task<T[]> LoadAsync<T>(IEnumerable<string> ids, CancellationToken token = default(CancellationToken))
        {
            var loadOeration = new LoadOperation(this);
            loadOeration.ByIds(ids);

            var command = loadOeration.CreateRequest();
            if (command != null)
            {
                await RequestExecuter.ExecuteAsync(command, Context);
                loadOeration.SetResult(command.Result);
            }

            return loadOeration.GetDocuments<T>();
        }

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

        private async Task<BlittableJsonReaderObject> TempAsyncDatabaseCommandGet(DocumentInfo documentInfo)
        {
            var command = new GetDocumentCommand
            {
                Ids = new[] { documentInfo.Id }
            };
            await RequestExecuter.ExecuteAsync(command, Context);
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

        /// <summary>
        /// Dynamically queries RavenDB using LINQ
        /// </summary>
        /// <typeparam name="T">The result of the query</typeparam>
        public IRavenQueryable<T> Query<T>()
        {
            string indexName = CreateDynamicIndexName<T>();

            return Query<T>(indexName);
        }

        public IRavenQueryable<T> Query<T, TIndexCreator>() where TIndexCreator : AbstractIndexCreationTask, new()
        {
            var indexCreator = new TIndexCreator();
            return Query<T>(indexCreator.IndexName, indexCreator.IsMapReduce);
        }

        public IRavenQueryable<T> Query<T>(string indexName, bool isMapReduce = false)
        {
            var ravenQueryStatistics = new RavenQueryStatistics();
            var highlightings = new RavenQueryHighlightings();
            var ravenQueryInspector = new RavenQueryInspector<T>();
            var ravenQueryProvider = new RavenQueryProvider<T>(this, indexName, ravenQueryStatistics, highlightings, null, AsyncDatabaseCommands, isMapReduce);
            ravenQueryInspector.Init(ravenQueryProvider,
                ravenQueryStatistics,
                highlightings,
                indexName,
                null,
                this, null, AsyncDatabaseCommands, isMapReduce);
            return ravenQueryInspector;
        }

        public IAsyncDocumentQuery<T> AsyncQuery<T>(string indexName, bool isMapReduce)
        {
            return AsyncDocumentQuery<T>(indexName, isMapReduce);
        }

        /// <summary>
        /// Query the specified index using Lucene syntax
        /// </summary>
        public IAsyncDocumentQuery<T> AsyncDocumentQuery<T>(string index, bool isMapReduce)
        {
            return new AsyncDocumentQuery<T>(this, null, AsyncDatabaseCommands, index, new string[0], new string[0], isMapReduce);
        }

        public RavenQueryInspector<S> CreateRavenQueryInspector<S>()
        {
            return new RavenQueryInspector<S>();
        }

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
                await RequestExecuter.ExecuteAsync(command, Context);
                saveChangesOeration.SetResult(command.Result);
            }
        }

        IDocumentQuery<T> IDocumentQueryGenerator.Query<T>(string indexName, bool isMapReduce)
        {
            throw new NotSupportedException("You can't query sync from an async session");
        }

        public void Defer(params ICommandData[] commands)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Get the accessor for advanced operations
        /// </summary>
        /// <remarks>
        /// Those operations are rarely needed, and have been moved to a separate 
        /// property to avoid cluttering the API
        /// </remarks>
        public IAdvancedDocumentSessionOperations Advanced
        {
            get { return this; }
        }


    }
}
