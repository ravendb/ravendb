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
using Raven.NewClient.Client.Connection;
using Raven.NewClient.Client.Connection.Async;
using Raven.NewClient.Client.Document.SessionOperations;
using Raven.NewClient.Client.Linq;
using Raven.NewClient.Client.Indexes;
using Raven.NewClient.Json.Linq;
using Raven.NewClient.Client.Document.Batches;
using System.Diagnostics;
using System.Dynamic;
using Raven.Abstractions.Commands;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Data.Queries;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Documents.Commands;
using Raven.NewClient.Client.Documents.SessionOperations;
using Raven.NewClient.Client.Http;
using Sparrow.Json;
using LoadOperation = Raven.NewClient.Client.Documents.SessionOperations.LoadOperation;
using Sparrow.Json.Parsing;

namespace Raven.NewClient.Client.Documents.Async
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

        public Lazy<Task<TResult>> LoadAsync<TResult>(string id, Type transformerType, Action<ILoadConfiguration> configure = null, Action<TResult> onEval = null,
            CancellationToken token = new CancellationToken())
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

        public Lazy<Task<TResult[]>> MoreLikeThisAsync<TResult>(MoreLikeThisQuery query, CancellationToken token = new CancellationToken())
        {
            throw new NotImplementedException();
        }

        Lazy<Task<TResult[]>> IAsyncLazySessionOperations.LoadStartingWithAsync<TResult>(string keyPrefix, string matches, int start, int pageSize,
            string exclude, RavenPagingInformation pagingInformation, string skipAfter,
            CancellationToken token)
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

        IAsyncLazyLoaderWithInclude<object> IAsyncLazySessionOperations.Include(string path)
        {
            throw new NotImplementedException();
        }

        Lazy<Task<TResult>> IAsyncLazySessionOperations.LoadAsync<TResult>(string id, CancellationToken token)
        {
            throw new NotImplementedException();
        }

        public Lazy<Task<T[]>> LazyAsyncLoadInternal<T>(string[] ids, KeyValuePair<string, Type>[] includes, Action<T[]> onEval, CancellationToken token = default (CancellationToken))
        {
            throw new NotImplementedException();
        }
    }
}
