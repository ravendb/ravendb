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
using Raven.Client.Json;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Session
{
    public partial class AsyncDocumentSession
    {
        public class YieldStream<T> : IAsyncEnumerator<StreamResult<T>>
        {
            private readonly AsyncDocumentSession _parent;
            private readonly IAsyncEnumerator<BlittableJsonReaderObject> _enumerator;
            private readonly AsyncDocumentQuery<T> _query;
            private readonly FieldsToFetchToken _fieldsToFetch;
            private readonly CancellationToken _token;
            private BlittableJsonReaderObject _prev;

            public YieldStream(AsyncDocumentSession parent, AsyncDocumentQuery<T> query, FieldsToFetchToken fieldsToFetch, IAsyncEnumerator<BlittableJsonReaderObject> enumerator, CancellationToken token)
            {
                _parent = parent;
                _enumerator = enumerator;
                _token = token;
                _query = query;
                _fieldsToFetch = fieldsToFetch;
            }

            public StreamResult<T> Current { get; protected set; }

            public void Dispose()
            {
                AsyncHelpers.RunSync(() => DisposeAsync().AsTask());
            }

            public ValueTask DisposeAsync()
            {
                return _enumerator.DisposeAsync();
            }

            public async ValueTask<bool> MoveNextAsync()
            {
                _prev?.Dispose(); // dispose the previous instance

                var isProjectInto = _query?.IsProjectInto ?? false;

                while (true)
                {
                    if (await _enumerator.MoveNextAsync().AsTask().WithCancellation(_token).ConfigureAwait(false) == false)
                        return false;

                    _prev = _enumerator.Current;

                    _query?.InvokeAfterStreamExecuted(_enumerator.Current);

                    Current = CreateStreamResult(_enumerator.Current, isProjectInto);
                    return true;
                }
            }

            private StreamResult<T> CreateStreamResult(BlittableJsonReaderObject json, bool isProjectInto)
            {
                var metadata = json.GetMetadata();
                var changeVector = metadata.GetChangeVector();
                //MapReduce indexes return reduce results that don't have @id property
                metadata.TryGetId(out string id);
                var entity = QueryOperation.Deserialize<T>(id, json, metadata, _fieldsToFetch, true, _parent, isProjectInto);

                var streamResult = new StreamResult<T>
                {
                    ChangeVector = changeVector,
                    Id = id,
                    Document = entity,
                    Metadata = new MetadataAsDictionary(metadata)
                };
                return streamResult;
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
                await RequestExecutor.ExecuteAsync(command, Context, SessionInfo, token).ConfigureAwait(false);
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

                await RequestExecutor.ExecuteAsync(command, Context, SessionInfo, token).ConfigureAwait(false);

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
                await RequestExecutor.ExecuteAsync(command, Context, SessionInfo, token).ConfigureAwait(false);
                var result = await streamOperation.SetResultAsync(command.Result).ConfigureAwait(false);

                var queryOperation = ((AsyncDocumentQuery<T>)query).InitializeQueryOperation();
                queryOperation.NoTracking = true;
                return new YieldStream<T>(this, documentQuery, fieldsToFetch, result, token);
            }
        }
    }
}
