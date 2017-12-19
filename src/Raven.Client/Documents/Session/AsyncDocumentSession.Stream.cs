using System;
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
            private readonly IAsyncDocumentQuery<T> _query;
            private readonly FieldsToFetchToken _fieldsToFetch;
            private readonly CancellationToken _token;

            public YieldStream(AsyncDocumentSession parent, IAsyncDocumentQuery<T> query, FieldsToFetchToken fieldsToFetch, IAsyncEnumerator<BlittableJsonReaderObject> enumerator, CancellationToken token)
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
                _enumerator.Dispose();
            }

            public async Task<bool> MoveNextAsync()
            {
                while (true)
                {
                    if (await _enumerator.MoveNextAsync().WithCancellation(_token).ConfigureAwait(false) == false)
                        return false;

                    _query?.InvokeAfterStreamExecuted(_enumerator.Current);

                    Current = CreateStreamResult(_enumerator.Current);
                    return true;
                }
            }

            private StreamResult<T> CreateStreamResult(BlittableJsonReaderObject json)
            {
                var metadata = json.GetMetadata();
                var changeVector = metadata.GetChangeVector();
                //MapReduce indexes return reduce results that don't have @id property
                metadata.TryGetId(out string id);
                var entity = QueryOperation.Deserialize<T>(id, json, metadata, _fieldsToFetch, true, _parent);

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

        public Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(IAsyncRawDocumentQuery<T> query, CancellationToken token = new CancellationToken())
        {
            return StreamAsync((IAsyncDocumentQuery<T>)query, token);
        }

        public Task StreamIntoAsync<T>(IAsyncRawDocumentQuery<T> query, Stream output, CancellationToken token = new CancellationToken())
        {
            return StreamIntoAsync((IAsyncDocumentQuery<T>)query, output, token);
        }

        public async Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(IAsyncDocumentQuery<T> query, CancellationToken token = default(CancellationToken))
        {
            var documentQuery = (AsyncDocumentQuery<T>)query;
            var fieldsToFetch = documentQuery.FieldsToFetchToken;
            var indexQuery = query.GetIndexQuery();

            var streamOperation = new StreamOperation(this);
            var command = streamOperation.CreateRequest(indexQuery);
            await RequestExecutor.ExecuteAsync(command, Context, SessionInfo, token).ConfigureAwait(false);
            var result = streamOperation.SetResultAsync(command.Result);

            var queryOperation = ((AsyncDocumentQuery<T>)query).InitializeQueryOperation();
            queryOperation.DisableEntitiesTracking = true;
            return new YieldStream<T>(this, query, fieldsToFetch, result, token);
        }

        public Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(IQueryable<T> query, CancellationToken token = default(CancellationToken))
        {
            var queryInspector = (IRavenQueryProvider)query.Provider;
            var indexQuery = queryInspector.ToAsyncDocumentQuery<T>(query.Expression);
            return StreamAsync(indexQuery, token);
        }

        public async Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(string startsWith, string matches = null, int start = 0,
                                   int pageSize = Int32.MaxValue, string startAfter = null, CancellationToken token = default(CancellationToken))
        {
            var streamOperation = new StreamOperation(this);
            var command = streamOperation.CreateRequest(startsWith, matches, start, pageSize, null, startAfter);
            await RequestExecutor.ExecuteAsync(command, Context, SessionInfo, token).ConfigureAwait(false);
            var result = streamOperation.SetResultAsync(command.Result);
            return new YieldStream<T>(this, null, null, result, token);
        }

        public async Task StreamIntoAsync<T>(IAsyncDocumentQuery<T> query, Stream output, CancellationToken token = default(CancellationToken))
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
}
