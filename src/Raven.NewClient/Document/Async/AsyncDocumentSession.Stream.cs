using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.NewClient.Abstractions.Extensions;
using Raven.NewClient.Abstractions.Util;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Connection;
using Raven.NewClient.Client.Linq;
using Sparrow.Json;
using Raven.NewClient.Client.Blittable;
using Raven.NewClient.Extensions;

namespace Raven.NewClient.Client.Document.Async
{
    public partial class AsyncDocumentSession
    {
        public class YieldStream<T> : IAsyncEnumerator<StreamResult<T>>
        {
            private readonly AsyncDocumentSession _parent;
            private readonly IAsyncEnumerator<BlittableJsonReaderObject> _enumerator;
            private readonly IAsyncDocumentQuery<T> _query;
            private readonly CancellationToken _token;

            public YieldStream(AsyncDocumentSession parent, IAsyncDocumentQuery<T> query, IAsyncEnumerator<BlittableJsonReaderObject> enumerator, CancellationToken token)
            {
                _parent = parent;
                _enumerator = enumerator;
                _token = token;
                _query = query;
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
                var entity = _parent.ConvertToEntity(typeof(T), id, json);
                var streamResult = new StreamResult<T>
                {
                    Etag = etag,
                    Key = id,
                    Document = (T)entity,
                    Metadata = new MetadataAsDictionary(metadata)
                };
                return streamResult;
            }

            public StreamResult<T> Current { get; protected set; }
        }

        public async Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(IAsyncDocumentQuery<T> query, CancellationToken token = default(CancellationToken))
        {
            var streamOperation = new StreamOperation(this);
            var command = streamOperation.CreateRequest((IRavenQueryInspector)query);
            await RequestExecuter.ExecuteAsync(command, Context, token).ConfigureAwait(false);
            var result = streamOperation.SetResultAsync(command.Result);

            var queryOperation = ((AsyncDocumentQuery<T>)query).InitializeQueryOperation();
            queryOperation.DisableEntitiesTracking = true;
            return new YieldStream<T>(this, query, result, token);
        }

        public async Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(IQueryable<T> query, CancellationToken token = default(CancellationToken))
        {
            var queryInspector = (IRavenQueryProvider)query.Provider;
            var indexQuery = queryInspector.ToAsyncDocumentQuery<T>(query.Expression);
            return await StreamAsync(indexQuery, token).ConfigureAwait(false);
        }

        public Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(long? fromEtag, int start = 0,
                                                                    int pageSize = Int32.MaxValue, RavenPagingInformation pagingInformation = null, string transformer = null, Dictionary<string, object> transformerParameters = null, CancellationToken token = default(CancellationToken))
        {
            return StreamAsync<T>(fromEtag: fromEtag, startsWith: null, matches: null, start: start, pageSize: pageSize, pagingInformation: pagingInformation, transformer: transformer, transformerParameters: transformerParameters, token: token);
        }

        public Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(string startsWith, string matches = null, int start = 0,
                                   int pageSize = Int32.MaxValue, RavenPagingInformation pagingInformation = null, string skipAfter = null, string transformer = null, Dictionary<string, object> transformerParameters = null, CancellationToken token = default(CancellationToken))
        {
            return StreamAsync<T>(fromEtag: null, startsWith: startsWith, matches: matches, start: start, pageSize: pageSize, pagingInformation: pagingInformation, skipAfter: skipAfter, transformer: transformer, transformerParameters: transformerParameters, token: token);
        }

        private async Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(long? fromEtag, string startsWith, string matches,
            int start, int pageSize, RavenPagingInformation pagingInformation = null, string skipAfter = null, string transformer = null,
            Dictionary<string, object> transformerParameters = null, CancellationToken token = default(CancellationToken))
        {
            var streamOperation = new StreamOperation(this);
            var command = streamOperation.CreateRequest(fromEtag, startsWith, matches, start, pageSize, null, pagingInformation, skipAfter, transformer,
                transformerParameters);
            await RequestExecuter.ExecuteAsync(command, Context, token).ConfigureAwait(false);
            var result = streamOperation.SetResultAsync(command.Result);
            return new YieldStream<T>(this, null, result, token);
        }
    }
}