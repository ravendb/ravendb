//-----------------------------------------------------------------------
// <copyright file="AsyncDocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands.MultiGet;
using Raven.Client.Documents.Session.Loaders;
using Raven.Client.Documents.Session.Operations;
using Raven.Client.Documents.Session.Operations.Lazy;
using Sparrow.Utils;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Implementation for async document session 
    /// </summary>
    public partial class AsyncDocumentSession
    {
        internal Lazy<Task<T>> AddLazyOperation<T>(ILazyOperation operation, Action<T> onEval, CancellationToken token = default(CancellationToken))
        {
            PendingLazyOperations.Add(operation);
            var lazyValue = new Lazy<Task<T>>(() =>
                ExecuteAllPendingLazyOperationsAsync(token)
                    .ContinueWith(t =>
                    {
                        if (t.Exception != null)
                            throw new InvalidOperationException("Could not perform add lazy operation", t.Exception);

                        return GetOperationResult<T>(operation.Result);
                    }, token));

            if (onEval != null)
                OnEvaluateLazy[operation] = theResult => onEval(GetOperationResult<T>(theResult));

            return lazyValue;
        }

        internal Lazy<Task<int>> AddLazyCountOperation(ILazyOperation operation, CancellationToken token = default(CancellationToken))
        {
            PendingLazyOperations.Add(operation);
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
            using (AsyncTaskHolder())
            {
                var requests = new List<GetRequest>();
                for (int i = 0; i < PendingLazyOperations.Count; i++)
                {
                    var req = PendingLazyOperations[i].CreateRequest(Context);
                    if (req == null)
                    {
                        PendingLazyOperations.RemoveAt(i);
                        i--; // so we'll recheck this index
                        continue;
                    }

                    requests.Add(req);
                }

                if (requests.Count == 0)
                    return new ResponseTimeInformation();

                try
                {
                    var sw = Stopwatch.StartNew();

                    var responseTimeDuration = new ResponseTimeInformation();

                    while (await ExecuteLazyOperationsSingleStep(responseTimeDuration, requests, sw, token).ConfigureAwait(false))
                    {
                        await TimeoutManager.WaitFor(TimeSpan.FromMilliseconds(100), token).ConfigureAwait(false);
                    }

                    responseTimeDuration.ComputeServerTotal();


                    foreach (var pendingLazyOperation in PendingLazyOperations)
                    {
                        Action<object> value;
                        if (OnEvaluateLazy.TryGetValue(pendingLazyOperation, out value))
                            value(pendingLazyOperation.Result);
                    }

                    sw.Stop();
                    responseTimeDuration.TotalClientDuration = sw.Elapsed;
                    return responseTimeDuration;
                }
                finally
                {
                    PendingLazyOperations.Clear();
                }
            }
        }

        private async Task<bool> ExecuteLazyOperationsSingleStep(ResponseTimeInformation responseTimeInformation, List<GetRequest> requests, Stopwatch sw, CancellationToken token = default)
        {
            var multiGetOperation = new MultiGetOperation(this);
            using var multiGetCommand = multiGetOperation.CreateRequest(requests);
            await RequestExecutor.ExecuteAsync(multiGetCommand, Context, sessionInfo: _sessionInfo, token: token).ConfigureAwait(false);
            var responses = multiGetCommand.Result;
            if (multiGetCommand.AggressivelyCached == false)
                IncrementRequestCount();

            for (var i = 0; i < PendingLazyOperations.Count; i++)
            {
                var response = responses[i];

                response.Headers.TryGetValue(Constants.Headers.RequestTime, out string tempReqTime);
                response.Elapsed = sw.Elapsed;

                long.TryParse(tempReqTime, out long totalTime);

                responseTimeInformation.DurationBreakdown.Add(new ResponseTimeItem
                {
                    Url = requests[i].UrlAndQuery,
                    Duration = TimeSpan.FromMilliseconds(totalTime)
                });

                if (response.RequestHasErrors())
                    throw new InvalidOperationException("Got an error from server, status code: " + (int)response.StatusCode + Environment.NewLine + response.Result);

                PendingLazyOperations[i].HandleResponse(response);
                if (PendingLazyOperations[i].RequiresRetry)
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

        public Lazy<Task<Dictionary<string, T>>> LazyAsyncLoadInternal<T>(string[] ids, string[] includes, Action<Dictionary<string, T>> onEval, CancellationToken token = default(CancellationToken))
        {
            if (CheckIfIdAlreadyIncluded(ids, includes))
            {
                return new Lazy<Task<Dictionary<string, T>>>(() => LoadAsync<T>(ids, token));
            }

            var loadOperation = new LoadOperation(this).ByIds(ids).WithIncludes(includes);
            var lazyOp = new LazyLoadOperation<T>(this, loadOperation).ByIds(ids).WithIncludes(includes);
            return AddLazyOperation(lazyOp, onEval, token);
        }

        Lazy<Task<Dictionary<string, T>>> IAsyncLazySessionOperations.LoadStartingWithAsync<T>(string idPrefix, string matches, int start, int pageSize,
            string exclude, string startAfter,
            CancellationToken token)
        {
            var operation = new LazyStartsWithOperation<T>(idPrefix, matches, exclude, start, pageSize, this, startAfter);

            return AddLazyOperation<Dictionary<string, T>>(operation, null, token);
        }

    }
}
