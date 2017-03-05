using System;
using System.Collections.Generic;
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

            public void Dispose()
            {
                _enumerator.Dispose();
            }

            public async Task<bool> MoveNextAsync()
            {
                if (await _enumerator.MoveNextAsync().WithCancellation(_token).ConfigureAwait(false) == false)
                {
                    return false;
                }
                SetCurrent();
                return true;
            }

            protected void SetCurrent()
            {
                var x = _enumerator.Current;
                _query?.InvokeAfterStreamExecuted(x);
                Current = CreateStreamResult<T>(x);
            }

            private StreamResult<T> CreateStreamResult<T>(BlittableJsonReaderObject json)
            {
                var metadata = json.GetMetadata();
                var etag = metadata.GetEtag();
                var id = metadata.GetId();

                json = _parent.Context.ReadObject(json, id);
                var entity = QueryOperation.Deserialize<T>(id, json, metadata, _projectionFields, true, _parent);

                var streamResult = new StreamResult<T>
                {
                    Etag = etag,
                    Key = id,
                    Document = entity,
                    Metadata = new MetadataAsDictionary(metadata)
                };
                return streamResult;
            }

            public StreamResult<T> Current { get; protected set; }
        }

        public async Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(IAsyncDocumentQuery<T> query, CancellationToken token = default(CancellationToken))
        {
            var documentQuery = (AsyncDocumentQuery<T>)query;
            var projectionFields = documentQuery.ProjectionFields;

            var streamOperation = new StreamOperation(this);
            var command = streamOperation.CreateRequest((IRavenQueryInspector)query);
            await RequestExecutor.ExecuteAsync(command, Context, token).ConfigureAwait(false);
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

        public Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(long? fromEtag, int start = 0,
                                                                    int pageSize = Int32.MaxValue, string transformer = null, Dictionary<string, object> transformerParameters = null, CancellationToken token = default(CancellationToken))
        {
            return StreamAsync<T>(fromEtag: fromEtag, startsWith: null, matches: null, start: start, pageSize: pageSize, transformer: transformer, transformerParameters: transformerParameters, token: token);
        }

        public Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(string startsWith, string matches = null, int start = 0,
                                   int pageSize = Int32.MaxValue, string startAfter = null, string transformer = null, Dictionary<string, object> transformerParameters = null, CancellationToken token = default(CancellationToken))
        {
            return StreamAsync<T>(fromEtag: null, startsWith: startsWith, matches: matches, start: start, pageSize: pageSize, startAfter: startAfter, transformer: transformer, transformerParameters: transformerParameters, token: token);
        }

        private async Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(long? fromEtag, string startsWith, string matches,
            int start, int pageSize, string startAfter = null, string transformer = null,
            Dictionary<string, object> transformerParameters = null, CancellationToken token = default(CancellationToken))
        {
            var streamOperation = new StreamOperation(this);
            var command = streamOperation.CreateRequest(fromEtag, startsWith, matches, start, pageSize, null, startAfter, transformer,
                transformerParameters);
            await RequestExecutor.ExecuteAsync(command, Context, token).ConfigureAwait(false);
            var result = streamOperation.SetResultAsync(command.Result);
            return new YieldStream<T>(this, null, null, result, token);
        }
    }
}