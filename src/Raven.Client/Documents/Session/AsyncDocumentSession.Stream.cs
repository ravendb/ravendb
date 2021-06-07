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
            private readonly StreamOperation.YieldStreamResults _enumerator;
            private readonly CancellationToken _token;
            private BlittableJsonReaderObject _prev;

            internal AbstractYieldStream(StreamOperation.YieldStreamResults enumerator, CancellationToken token)
            {
                _enumerator = enumerator;
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
                    Current = ResultCreator(_enumerator);
                    return true;
                }
            }

            internal abstract T ResultCreator(StreamOperation.YieldStreamResults asyncEnumerator);
        }

        public class YieldStream<T> : AbstractYieldStream<StreamResult<T>>
        {
            private readonly AsyncDocumentSession _parent;
            private readonly IAbstractDocumentQueryImpl<T> _query;
            private readonly FieldsToFetchToken _fieldsToFetch;

            internal YieldStream(
                AsyncDocumentSession parent, 
                StreamOperation.YieldStreamResults enumerator,
                IAbstractDocumentQueryImpl<T> query, 
                FieldsToFetchToken fieldsToFetch,
                CancellationToken token) :
                base(enumerator, token)
            {
                _parent = parent;
                _query = query;
                _fieldsToFetch = fieldsToFetch;
            }

            internal override StreamResult<T> ResultCreator(StreamOperation.YieldStreamResults asyncEnumerator)
            {
                var current = asyncEnumerator.Current;
                if (_query is IAsyncDocumentQuery<T> q)
                    q.InvokeAfterStreamExecuted(current);
                return _parent.CreateStreamResult<T>(current, _fieldsToFetch, _query?.IsProjectInto ?? false);
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
                var result = await streamOperation.SetResultAsync(command.Result, token).ConfigureAwait(false);
                return new YieldStream<T>(this, result, null, null, token);
            }
        }

        public async Task StreamIntoAsync<T>(IAsyncDocumentQuery<T> query, Stream output, CancellationToken token = default)
        {
            using (AsyncTaskHolder())
            {
                var streamOperation = new StreamOperation(this);
                var command = streamOperation.CreateRequest(query.GetIndexQuery());

                await RequestExecutor.ExecuteAsync(command, Context, _sessionInfo, token).ConfigureAwait(false);

                streamOperation.EnsureIsAcceptable(query.IndexName, command.Result);

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
                var documentQuery = (IAbstractDocumentQueryImpl<T>)query;
                var fieldsToFetch = documentQuery.FieldsToFetchToken;
                
                var queryOperation = documentQuery.InitializeQueryOperation();
                queryOperation.NoTracking = true;
                
                var indexQuery = query.GetIndexQuery();
                
                var streamOperation = new StreamOperation(this, streamQueryStats);
                var command = streamOperation.CreateRequest(indexQuery);
                await RequestExecutor.ExecuteAsync(command, Context, _sessionInfo, token).ConfigureAwait(false);
                streamOperation.EnsureIsAcceptable(query.IndexName, command.Result);

                var result = await streamOperation.SetResultAsync(command.Result, token).ConfigureAwait(false);
                
                return new YieldStream<T>(this, result, documentQuery, fieldsToFetch, token);
            }
        }
    }
}
