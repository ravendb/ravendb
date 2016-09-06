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
using Raven.Client.Data;
using Raven.Client.Data.Queries;
using Raven.Client.Document;
using Raven.Client.Documents.SessionOperations;
using Raven.Client.Http;
using Sparrow.Json;
using LoadOperation = Raven.Client.Documents.SessionOperations.LoadOperation;

namespace Raven.Client.Documents.Async
{
    /// <summary>
    /// Implementation for async document session 
    /// </summary>
    public class AsyncDocumentSession : InMemoryDocumentSessionOperations
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
            asyncDocumentKeyGeneration = new AsyncDocumentKeyGeneration(this, DocumentsAndMetadata.TryGetValue, (key, entity, metadata) => key);
        }

        /// <summary>
        /// Gets the async database commands.
        /// </summary>
        /// <value>The async database commands.</value>
        public IAsyncDatabaseCommands AsyncDatabaseCommands { get; private set; }

        public string GetDocumentUrl(object entity)
        {
            DocumentMetadata value;
            if (DocumentsAndMetadata.TryGetValue(entity, out value) == false)
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
        protected override JsonDocument GetJsonDocument(string documentKey)
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
            return Conventions.GenerateDocumentKeyAsync(databaseName, AsyncDatabaseCommands, entity);
        }

        public async Task<BlittableJsonReaderObject> GetMetadataForAsync<T>(T instance)
        {
            var metadata = await GetDocumentMetadataAsync(instance).ConfigureAwait(false);
            return metadata.Metadata;
        }

        private async Task<DocumentMetadata> GetDocumentMetadataAsync<T>(T instance)
        {
            DocumentMetadata value;
            if (DocumentsAndMetadata.TryGetValue(instance, out value) == false)
            {
                string id;
                if (GenerateEntityIdOnTheClient.TryGetIdFromInstance(instance, out id)
                    || (instance is IDynamicMetaObjectProvider &&
                       GenerateEntityIdOnTheClient.TryGetIdFromDynamic(instance, out id)))
                {
                    AssertNoNonUniqueInstance(instance, id);

                    var jsonDocument = await GetJsonDocumentAsync(id).ConfigureAwait(false);

                    value = GetDocumentMetadataValue(instance, id, jsonDocument);
                }
                else
                {
                    throw new InvalidOperationException("Could not find the document key for " + instance);
                }
            }
            return value;
        }

        /// <summary>
        /// Get the json document by key from the store
        /// </summary>
        private async Task<JsonDocument> GetJsonDocumentAsync(string documentKey)
        {
            var jsonDocument = await AsyncDatabaseCommands.GetAsync(documentKey).ConfigureAwait(false);
            if (jsonDocument == null)
                throw new InvalidOperationException("Document '" + documentKey + "' no longer exists and was probably deleted");
            return jsonDocument;
        }
    }
}
