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
    public class AsyncDocumentSession : InMemoryDocumentSessionOperations, IAsyncDocumentSessionImpl, IAsyncAdvancedSessionOperations, IDocumentQueryGenerator
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

        public Task<IEnumerable<T>> LoadStartingWithAsync<T>(string keyPrefix, string matches = null, int start = 0, int pageSize = 25, string exclude = null, RavenPagingInformation pagingInformation = null, string skipAfter = null, CancellationToken token = default(CancellationToken))
        {
            throw new NotImplementedException();
        }

        public Task<IEnumerable<TResult>> LoadStartingWithAsync<TTransformer, TResult>(string keyPrefix, string matches = null, int start = 0,
            int pageSize = 25, string exclude = null, RavenPagingInformation pagingInformation = null, Action<ILoadConfiguration> configure = null,
            string skipAfter = null, CancellationToken token = new CancellationToken()) where TTransformer : AbstractTransformerCreationTask, new()
        {
            throw new NotImplementedException();
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

        public Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(IAsyncDocumentQuery<T> query, CancellationToken token = new CancellationToken())
        {
            throw new NotImplementedException();
        }

        public Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(IQueryable<T> query, CancellationToken token = new CancellationToken())
        {
            throw new NotImplementedException();
        }

        public Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(IAsyncDocumentQuery<T> query, Reference<QueryHeaderInformation> queryHeaderInformation,
            CancellationToken token = new CancellationToken())
        {
            throw new NotImplementedException();
        }

        public Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(IQueryable<T> query, Reference<QueryHeaderInformation> queryHeaderInformation,
            CancellationToken token = new CancellationToken())
        {
            throw new NotImplementedException();
        }

        public Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(long? fromEtag, int start = 0, int pageSize = Int32.MaxValue,
            RavenPagingInformation pagingInformation = null, string transformer = null, Dictionary<string, RavenJToken> transformerParameters = null,
            CancellationToken token = new CancellationToken())
        {
            throw new NotImplementedException();
        }

        public Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(string startsWith, string matches = null, int start = 0, int pageSize = Int32.MaxValue,
            RavenPagingInformation pagingInformation = null, string skipAfter = null, string transformer = null,
            Dictionary<string, RavenJToken> transformerParameters = null, CancellationToken token = new CancellationToken())
        {
            throw new NotImplementedException();
        }

        public Task<Operation> DeleteByIndexAsync<T, TIndexCreator>(Expression<Func<T, bool>> expression) where TIndexCreator : AbstractIndexCreationTask, new()
        {
            throw new NotImplementedException();
        }

        public Task<Operation> DeleteByIndexAsync<T>(string indexName, Expression<Func<T, bool>> expression)
        {
            throw new NotImplementedException();
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

        public Lazy<Task<TResult>> LoadAsync<TResult>(string id, Action<TResult> onEval, CancellationToken token = new CancellationToken())
        {
            throw new NotImplementedException();
        }

        Lazy<Task<TResult>> IAsyncLazySessionOperations.LoadAsync<TResult>(ValueType id, CancellationToken token)
        {
            throw new NotImplementedException();
        }

        public Lazy<Task<TResult>> LoadAsync<TResult>(ValueType id, Action<TResult> onEval, CancellationToken token = new CancellationToken())
        {
            throw new NotImplementedException();
        }

        Lazy<Task<TResult[]>> IAsyncLazySessionOperations.LoadAsync<TResult>(CancellationToken token, params ValueType[] ids)
        {
            throw new NotImplementedException();
        }

        Lazy<Task<TResult[]>> IAsyncLazySessionOperations.LoadAsync<TResult>(IEnumerable<ValueType> ids, CancellationToken token)
        {
            throw new NotImplementedException();
        }

        public Lazy<Task<TResult[]>> LoadAsync<TResult>(IEnumerable<ValueType> ids, Action<TResult[]> onEval, CancellationToken token = new CancellationToken())
        {
            throw new NotImplementedException();
        }

        public Lazy<Task<TResult>> LoadAsync<TTransformer, TResult>(string id, Action<ILoadConfiguration> configure = null, Action<TResult> onEval = null,
            CancellationToken token = new CancellationToken()) where TTransformer : AbstractTransformerCreationTask, new()
        {
            throw new NotImplementedException();
        }

        public Lazy<Task<TResult>> LoadAsync<TResult>(string id, Type transformerType, Action<ILoadConfiguration> configure = null, Action<TResult> onEval = null,
            CancellationToken token = new CancellationToken())
        {
            throw new NotImplementedException();
        }

        Lazy<Task<TResult[]>> IAsyncLazySessionOperations.LoadStartingWithAsync<TResult>(string keyPrefix, string matches, int start, int pageSize,
            string exclude, RavenPagingInformation pagingInformation, string skipAfter,
            CancellationToken token)
        {
            throw new NotImplementedException();
        }

        public Lazy<Task<TResult[]>> MoreLikeThisAsync<TResult>(MoreLikeThisQuery query, CancellationToken token = new CancellationToken())
        {
            throw new NotImplementedException();
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

        public Task<TResult> LoadAsync<TTransformer, TResult>(string id, Action<ILoadConfiguration> configure = null,
            CancellationToken token = new CancellationToken()) where TTransformer : AbstractTransformerCreationTask, new()
        {
            throw new NotImplementedException();
        }

        public Task<TResult[]> LoadAsync<TTransformer, TResult>(IEnumerable<string> ids, Action<ILoadConfiguration> configure = null,
            CancellationToken token = new CancellationToken()) where TTransformer : AbstractTransformerCreationTask, new()
        {
            throw new NotImplementedException();
        }

        public Task<TResult> LoadAsync<TResult>(string id, string transformer, Action<ILoadConfiguration> configure = null,
            CancellationToken token = new CancellationToken())
        {
            throw new NotImplementedException();
        }

        public Task<TResult[]> LoadAsync<TResult>(IEnumerable<string> ids, string transformer, Action<ILoadConfiguration> configure = null,
            CancellationToken token = new CancellationToken())
        {
            throw new NotImplementedException();
        }

        public Task<TResult> LoadAsync<TResult>(string id, Type transformerType, Action<ILoadConfiguration> configure = null,
            CancellationToken token = new CancellationToken())
        {
            throw new NotImplementedException();
        }

        public Task<TResult[]> LoadAsync<TResult>(IEnumerable<string> ids, Type transformerType, Action<ILoadConfiguration> configure = null,
            CancellationToken token = new CancellationToken())
        {
            throw new NotImplementedException();
        }

        public IAsyncAdvancedSessionOperations Advanced { get; }

        IAsyncLoaderWithInclude<object> IAsyncDocumentSession.Include(string path)
        {
            throw new NotImplementedException();
        }

        IAsyncLazyLoaderWithInclude<TResult> IAsyncLazySessionOperations.Include<TResult>(Expression<Func<TResult, object>> path)
        {
            throw new NotImplementedException();
        }

        Lazy<Task<TResult[]>> IAsyncLazySessionOperations.LoadAsync<TResult>(IEnumerable<string> ids, CancellationToken token)
        {
            throw new NotImplementedException();
        }

        public Lazy<Task<TResult[]>> LoadAsync<TResult>(IEnumerable<string> ids, Action<TResult[]> onEval, CancellationToken token = new CancellationToken())
        {
            throw new NotImplementedException();
        }

        Lazy<Task<TResult>> IAsyncLazySessionOperations.LoadAsync<TResult>(string id, CancellationToken token)
        {
            throw new NotImplementedException();
        }

        IAsyncLazyLoaderWithInclude<object> IAsyncLazySessionOperations.Include(string path)
        {
            throw new NotImplementedException();
        }

        IAsyncLoaderWithInclude<T> IAsyncDocumentSession.Include<T>(Expression<Func<T, object>> path)
        {
            throw new NotImplementedException();
        }

        public IAsyncLoaderWithInclude<T> Include<T, TInclude>(Expression<Func<T, object>> path)
        {
            throw new NotImplementedException();
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

        public IAsyncEagerSessionOperations Eagerly { get; }
        public IAsyncLazySessionOperations Lazily { get; }

        public IAsyncDocumentQuery<T> AsyncDocumentQuery<T, TIndexCreator>() where TIndexCreator : AbstractIndexCreationTask, new()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Query the specified index using Lucene syntax
        /// </summary>
        public IAsyncDocumentQuery<T> AsyncDocumentQuery<T>(string index, bool isMapReduce)
        {
            return new AsyncDocumentQuery<T>(this, null, AsyncDatabaseCommands, index, new string[0], new string[0], isMapReduce);
        }

        public IAsyncDocumentQuery<T> AsyncDocumentQuery<T>()
        {
            throw new NotImplementedException();
        }

        public IAsyncDocumentQuery<T> AsyncLuceneQuery<T, TIndexCreator>() where TIndexCreator : AbstractIndexCreationTask, new()
        {
            throw new NotImplementedException();
        }

        public IAsyncDocumentQuery<T> AsyncLuceneQuery<T>(string index, bool isMapReduce = false)
        {
            throw new NotImplementedException();
        }

        public IAsyncDocumentQuery<T> AsyncLuceneQuery<T>()
        {
            throw new NotImplementedException();
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

        public Task<T[]> LoadAsyncInternal<T>(string[] ids, KeyValuePair<string, Type>[] includes, CancellationToken token = new CancellationToken())
        {
            throw new NotImplementedException();
        }

        public async Task<T[]> LoadUsingTransformerInternalAsync<T>(string[] ids, KeyValuePair<string, Type>[] includes, string transformer, Dictionary<string, RavenJToken> transformerParameters = null, CancellationToken token = default(CancellationToken))
        {
            throw new NotImplementedException();
        }

        public Lazy<Task<T[]>> LazyAsyncLoadInternal<T>(string[] ids, KeyValuePair<string, Type>[] includes, Action<T[]> onEval, CancellationToken token = default (CancellationToken))
        {
            throw new NotImplementedException();
        }
    }
}
