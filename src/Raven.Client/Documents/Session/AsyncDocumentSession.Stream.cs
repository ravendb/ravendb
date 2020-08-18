using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Session.Operations;
using Raven.Client.Documents.Session.Tokens;
using Raven.Client.Extensions;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Session
{
    public partial class AsyncDocumentSession
    {
        public abstract class AbstractYieldStream<T> : IAsyncEnumerator<T>
        {
            private readonly IAsyncEnumerator<BlittableJsonReaderObject> _enumerator;
            private readonly Func<T> _resultCreator;
            private readonly CancellationToken _token;
            private BlittableJsonReaderObject _prev;

            protected AbstractYieldStream(IAsyncEnumerator<BlittableJsonReaderObject> enumerator, Func<T> resultCreator, CancellationToken token)
            {
                _enumerator = enumerator;
                _resultCreator = resultCreator;
                _token = token;
            }

            public T Current { get; protected set; }

            public void Dispose()
            {
                var dispose = DisposeAsync();
                if (dispose.IsCompletedSuccessfully)
                    return;

                AsyncHelpers.RunSync(() => dispose.AsTask());
            }

            public ValueTask DisposeAsync()
            {
                return _enumerator.DisposeAsync();
            }

            public async ValueTask<bool> MoveNextAsync()
            {
                _prev?.Dispose(); // dispose the previous instance

                while (true)
                {
                    var next = _enumerator.MoveNextAsync();
                    if (next.IsCompleted)
                    {
                        if (next.Result == false)
                            return false;
                    }
                    else if (await next.AsTask().WithCancellation(_token).ConfigureAwait(false) == false)
                        return false;

                    _prev = _enumerator.Current;

                    Current = _resultCreator.Invoke();
                    return true;
                }
            }
        }

        public class YieldStream<T> : AbstractYieldStream<StreamResult<T>>
        {
            internal YieldStream(AsyncDocumentSession parent, AsyncDocumentQuery<T> query, FieldsToFetchToken fieldsToFetch, IAsyncEnumerator<BlittableJsonReaderObject> enumerator, CancellationToken token) : 
                base(enumerator, () =>
                {
                    query?.InvokeAfterStreamExecuted(enumerator.Current);
                    return parent.CreateStreamResult<T>(enumerator.Current, fieldsToFetch, query?.IsProjectInto ?? false);
                }, token)
            {
            }
        }

        public Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(IAsyncRawDocumentQuery<T> query, CancellationToken token = default)
        {
            return StreamAsync((IAsyncDocumentQuery<T>)query, token);
        }

        public Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(IAsyncRawDocumentQuery<T> query, out StreamQueryStatistics streamQueryStats, CancellationToken token = default)
        {
            return StreamAsync((IAsyncDocumentQuery<T>)query, out streamQueryStats, token);
        }

        public Task StreamIntoAsync<T>(IAsyncRawDocumentQuery<T> query, Stream output, CancellationToken token = default)
        {
            return StreamIntoAsync((IAsyncDocumentQuery<T>)query, output, token);
        }

        public Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(IAsyncDocumentQuery<T> query, CancellationToken token = default)
        {
            return PerformQueryStreamOperation(query, null, token);
        }

        public Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(IAsyncDocumentQuery<T> query, out StreamQueryStatistics streamQueryStats, CancellationToken token = default)
        {
            streamQueryStats = new StreamQueryStatistics();
            return PerformQueryStreamOperation(query, streamQueryStats, token);
        }

        public Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(IQueryable<T> query, CancellationToken token = default)
        {
            var queryInspector = (IRavenQueryProvider)query.Provider;
            var indexQuery = queryInspector.ToAsyncDocumentQuery<T>(query.Expression);
            return StreamAsync(indexQuery, token);
        }

        public Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(IQueryable<T> query, out StreamQueryStatistics streamQueryStats, CancellationToken token = default)
        {
            var queryInspector = (IRavenQueryProvider)query.Provider;
            var indexQuery = queryInspector.ToAsyncDocumentQuery<T>(query.Expression);
            return StreamAsync(indexQuery, out streamQueryStats, token);
        }

        public async Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(string startsWith, string matches = null, int start = 0,
                                   int pageSize = int.MaxValue, string startAfter = null, CancellationToken token = default)
        {
            using (AsyncTaskHolder())
            {
                var streamOperation = new StreamOperation(this);
                var command = streamOperation.CreateRequest(startsWith, matches, start, pageSize, null, startAfter);
                await RequestExecutor.ExecuteAsync(command, Context, _sessionInfo, token).ConfigureAwait(false);
                var result = await streamOperation.SetResultAsync(command.Result).ConfigureAwait(false);
                return new YieldStream<T>(this, null, null, result, token);
            }
        }

        public async Task StreamIntoAsync<T>(IAsyncDocumentQuery<T> query, Stream output, CancellationToken token = default)
        {
            using (AsyncTaskHolder())
            {
                var streamOperation = new StreamOperation(this);
                var command = streamOperation.CreateRequest(query.GetIndexQuery());

                await RequestExecutor.ExecuteAsync(command, Context, _sessionInfo, token).ConfigureAwait(false);

                using (command.Result.Response)
                using (command.Result.Stream)
                {
                    await command.Result.Stream.CopyToAsync(output, 16 * 1024, token).ConfigureAwait(false);
                }
            }
        }

        private async Task<IAsyncEnumerator<StreamResult<T>>> PerformQueryStreamOperation<T>(IAsyncDocumentQuery<T> query, StreamQueryStatistics streamQueryStats, CancellationToken token)
        {
            using (AsyncTaskHolder())
            {
                var documentQuery = (AsyncDocumentQuery<T>)query;
                var fieldsToFetch = documentQuery.FieldsToFetchToken;
                var indexQuery = query.GetIndexQuery();

                var streamOperation = new StreamOperation(this, streamQueryStats);
                var command = streamOperation.CreateRequest(indexQuery);
                await RequestExecutor.ExecuteAsync(command, Context, _sessionInfo, token).ConfigureAwait(false);
                var result = await streamOperation.SetResultAsync(command.Result).ConfigureAwait(false);

                var queryOperation = ((AsyncDocumentQuery<T>)query).InitializeQueryOperation();
                queryOperation.NoTracking = true;
                return new YieldStream<T>(this, documentQuery, fieldsToFetch, result, token);
            }
        }
    }
}
