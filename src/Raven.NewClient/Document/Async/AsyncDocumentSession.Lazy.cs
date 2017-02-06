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
using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Abstractions.Extensions;
using Raven.NewClient.Client.Linq;
using Raven.NewClient.Client.Indexes;
using Raven.NewClient.Client.Document.Batches;
using System.Diagnostics;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Commands.Lazy;
using Raven.NewClient.Client.Data.Queries;

namespace Raven.NewClient.Client.Document.Async
{
    /// <summary>
    /// Implementation for async document session 
    /// </summary>
    public partial class AsyncDocumentSession : InMemoryDocumentSessionOperations, IAsyncDocumentSessionImpl, IAsyncAdvancedSessionOperations, IDocumentQueryGenerator
    {
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
                }, token));

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
            var requests = pendingLazyOperations.Select(x => x.CreateRequest()).ToList();
            var multiGetOperation = new MultiGetOperation(this);
            var multiGetCommand = multiGetOperation.CreateRequest(requests);
            await RequestExecuter.ExecuteAsync(multiGetCommand, Context).ConfigureAwait(false);
            var responses = multiGetCommand.Result;

            for (var i = 0; i < pendingLazyOperations.Count; i++)
            {
                long totalTime;
                string tempReqTime;
                var response = responses[i];

                response.Headers.TryGetValue(Constants.Headers.RequestTime, out tempReqTime);

                long.TryParse(tempReqTime, out totalTime);

                responseTimeInformation.DurationBreakdown.Add(new ResponseTimeItem
                {
                    Url = requests[i].UrlAndQuery,
                    Duration = TimeSpan.FromMilliseconds(totalTime)
                });

                if (response.RequestHasErrors())
                    throw new InvalidOperationException("Got an error from server, status code: " + (int)response.StatusCode + Environment.NewLine + response.Result);

                pendingLazyOperations[i].HandleResponse(response);
                if (pendingLazyOperations[i].RequiresRetry)
                {
                    return true;
                }
            }
            return false;
        }

        /// <summary>
        /// Begin a load while including the specified path 
        /// </summary>
        /// <param name="path">The path.</param>
        IAsyncLazyLoaderWithInclude<T> IAsyncLazySessionOperations.Include<T>(Expression<Func<T, string>> path)
        {
            return new AsyncLazyMultiLoaderWithInclude<T>(this).Include(path);
        }

        /// <summary>
        /// Begin a load while including the specified path 
        /// </summary>
        /// <param name="path">The path.</param>
        IAsyncLazyLoaderWithInclude<T> IAsyncLazySessionOperations.Include<T>(Expression<Func<T, IEnumerable<string>>> path)
        {
            return new AsyncLazyMultiLoaderWithInclude<T>(this).Include(path);
        }

        /// <summary>
        /// Begin a load while including the specified path 
        /// </summary>
        /// <param name="path">The path.</param>
        IAsyncLazyLoaderWithInclude<object> IAsyncLazySessionOperations.Include(string path)
        {
            return new AsyncLazyMultiLoaderWithInclude<object>(this).Include(path);
        }

        /// <summary>
        /// Loads the specified ids.
        /// </summary>
        /// <param name="token">The cancellation token.</param>
        /// <param name="ids">The ids of the documents to load.</param>
        Lazy<Task<Dictionary<string, T>>> IAsyncLazySessionOperations.LoadAsync<T>(IEnumerable<string> ids, CancellationToken token)
        {
            return Lazily.LoadAsync<T>(ids, null, token);
        }

        Lazy<Task<T>> IAsyncLazySessionOperations.LoadAsync<T>(string id, CancellationToken token)
        {
            return Lazily.LoadAsync(id, (Action<T>)null, token);
        }
        
        /// <summary>
        /// Loads the specified ids and a function to call when it is evaluated
        /// </summary>
        public Lazy<Task<Dictionary<string, T>>> LoadAsync<T>(IEnumerable<string> ids, Action<Dictionary<string, T>> onEval, CancellationToken token = new CancellationToken())
        {
            return LazyAsyncLoadInternal(ids.ToArray(), new string[0], onEval, token);
        }

        /// <summary>
        /// Loads the specified id and a function to call when it is evaluated
        /// </summary>
        public Lazy<Task<T>> LoadAsync<T>(string id, Action<T> onEval, CancellationToken token = new CancellationToken())
        {
            if (IsLoaded(id))
                return new Lazy<Task<T>>(() => LoadAsync<T>(id, token));

            var lazyLoadOperation = new LazyLoadOperation<T>(this, new LoadOperation(this).ById(id)).ById(id);
            return AddLazyOperation(lazyLoadOperation, onEval, token);
        }

        Lazy<Task<List<T>>> IAsyncLazySessionOperations.MoreLikeThisAsync<T>(MoreLikeThisQuery query, CancellationToken token = new CancellationToken())
        {
            var lazyOp = new LazyMoreLikeThisOperation<T>(this, query);
            return AddLazyOperation<List<T>>(lazyOp, null, token);
        }

        public Lazy<Task<Dictionary<string, T>>> LazyAsyncLoadInternal<T>(string[] ids, string[] includes, Action<Dictionary<string, T>> onEval, CancellationToken token = default(CancellationToken))
        {
            var loadOperation = new LoadOperation(this).ByIds(ids).WithIncludes(includes);
            var lazyOp = new LazyLoadOperation<T>(this, loadOperation).ByIds(ids).WithIncludes(includes);
            return AddLazyOperation(lazyOp, onEval, token);
        }

        Lazy<Task<T[]>> IAsyncLazySessionOperations.LoadStartingWithAsync<T>(string keyPrefix, string matches, int start, int pageSize,
            string exclude, RavenPagingInformation pagingInformation, string skipAfter,
            CancellationToken token)
        {
            var operation = new LazyStartsWithOperation<T>(keyPrefix, matches, exclude, start, pageSize, this, pagingInformation, skipAfter);

            return AddLazyOperation<T[]>(operation, null, token);
        }

        Lazy<Task<TResult>> IAsyncLazySessionOperations.LoadAsync<TTransformer, TResult>(string id, Action<ILoadConfiguration> configure = null, Action<TResult> onEval = null,
            CancellationToken token = new CancellationToken())
        {
            return Lazily.LoadAsync(id, typeof(TTransformer), configure, onEval, token);
        }

        Lazy<Task<T>> IAsyncLazySessionOperations.LoadAsync<T>(string id, Type transformerType, Action<ILoadConfiguration> configure = null, Action<T> onEval = null,
            CancellationToken token = new CancellationToken())
        {
            var transformer = ((AbstractTransformerCreationTask)Activator.CreateInstance(transformerType)).TransformerName;
            var ids = new[] { id };

            var configuration = new RavenLoadConfiguration();
            if (configure != null)
                configure(configuration);

            var lazyLoadOperation = new LazyTransformerLoadOperation<T>(
                ids,
                transformer,
                configuration.TransformerParameters,
                new LoadTransformerOperation(this),
                singleResult: true);

            return AddLazyOperation(lazyLoadOperation, onEval, token);
        }
    }
}
