using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Session.Operations;
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
            private readonly string[] _projectionFields;
            private readonly CancellationToken _token;

            public YieldStream(AsyncDocumentSession parent, IAsyncDocumentQuery<T> query, string[] projectionFields, IAsyncEnumerator<BlittableJsonReaderObject> enumerator, CancellationToken token)
            {
                _parent = parent;
                _enumerator = enumerator;
                _token = token;
                _query = query;
                _projectionFields = projectionFields;
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
                var changeVector = BlittableJsonExtensions.GetChangeVector(metadata);
                var id = metadata.GetId();

                json = _parent.Context.ReadObject(json, id);
                var entity = QueryOperation.Deserialize<T>(id, json, metadata, _projectionFields, true, _parent);

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

        public async Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(IAsyncDocumentQuery<T> query, CancellationToken token = default(CancellationToken))
        {
            var documentQuery = (AsyncDocumentQuery<T>)query;
            var projectionFields = documentQuery.FieldsToFetchToken?.Projections;
            var indexQuery = query.GetIndexQuery();

            var streamOperation = new StreamOperation(this);
            var command = streamOperation.CreateRequest(query.IndexName, indexQuery);
            await RequestExecutor.ExecuteAsync(command, Context, token, sessionId: _clientSessionId).ConfigureAwait(false);
            var result = streamOperation.SetResultAsync(command.Result);

            var queryOperation = ((AsyncDocumentQuery<T>)query).InitializeQueryOperation();
            queryOperation.DisableEntitiesTracking = true;
            return new YieldStream<T>(this, query, projectionFields, result, token);
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
            await RequestExecutor.ExecuteAsync(command, Context, token, sessionId: _clientSessionId).ConfigureAwait(false);
            var result = streamOperation.SetResultAsync(command.Result);
            return new YieldStream<T>(this, null, null, result, token);
        }

        public async Task StreamIntoAsync<T>(IAsyncDocumentQuery<T> query, Stream output, CancellationToken token = default(CancellationToken))
        {
            var streamOperation = new StreamOperation(this);
            var command = streamOperation.CreateRequest(query.IndexName, query.GetIndexQuery());

            await RequestExecutor.ExecuteAsync(command, Context, token, sessionId: _clientSessionId).ConfigureAwait(false);

            using (command.Result.Response)
            using (command.Result.Stream)
            {
                await command.Result.Stream.CopyToAsync(output, 16 * 1024, token).ConfigureAwait(false);
            }
        }
    }
}
