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
using Raven.Client.Documents.Transformers;
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
            private readonly bool _usedTransformer;
            private readonly CancellationToken _token;
            private IEnumerator<StreamResult<T>> _innerEnumerator;

            public YieldStream(AsyncDocumentSession parent, IAsyncDocumentQuery<T> query, string[] projectionFields, bool usedTransformer, IAsyncEnumerator<BlittableJsonReaderObject> enumerator, CancellationToken token)
            {
                _parent = parent;
                _enumerator = enumerator;
                _token = token;
                _query = query;
                _projectionFields = projectionFields;
                _usedTransformer = usedTransformer;
            }

            public StreamResult<T> Current { get; protected set; }

            public void Dispose()
            {
                _enumerator.Dispose();
            }

            public async Task<bool> MoveNextAsync()
            {
                if (_usedTransformer && _innerEnumerator != null)
                {
                    if (_innerEnumerator.MoveNext())
                    {
                        Current = _innerEnumerator.Current;
                        return true;
                    }

                    _innerEnumerator.Dispose();
                    _innerEnumerator = null;
                }

                while (true)
                {
                    if (await _enumerator.MoveNextAsync().WithCancellation(_token).ConfigureAwait(false) == false)
                        return false;

                    _query?.InvokeAfterStreamExecuted(_enumerator.Current);

                    if (_usedTransformer)
                    {
                        Debug.Assert(_innerEnumerator == null);

                        _innerEnumerator = CreateMultipleStreamResults(_enumerator.Current).GetEnumerator();
                        if (_innerEnumerator.MoveNext() == false)
                        {
                            _innerEnumerator.Dispose();
                            _innerEnumerator = null;
                            continue;
                        }

                        Current = _innerEnumerator.Current;
                        return true;
                    }

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

            private IEnumerable<StreamResult<T>> CreateMultipleStreamResults(BlittableJsonReaderObject json)
            {
                BlittableJsonReaderArray values;
                if (json.TryGet(Constants.Json.Fields.Values, out values) == false)
                    throw new InvalidOperationException("Transformed document must have a $values property");

                var metadata = json.GetMetadata();
                var changeVector = BlittableJsonExtensions.GetChangeVector(metadata);
                var id = metadata.GetId();

                foreach (var value in TransformerHelper.ParseResultsForStreamOperation<T>(_parent, values))
                {
                    yield return new StreamResult<T>
                    {
                        Id = id,
                        ChangeVector = changeVector,
                        Document = value,
                        Metadata = new MetadataAsDictionary(metadata)
                    };
                }
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
            return new YieldStream<T>(this, query, projectionFields, command.UsedTransformer, result, token);
        }

        public Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(IQueryable<T> query, CancellationToken token = default(CancellationToken))
        {
            var queryInspector = (IRavenQueryProvider)query.Provider;
            var indexQuery = queryInspector.ToAsyncDocumentQuery<T>(query.Expression);
            return StreamAsync(indexQuery, token);
        }

        public async Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(string startsWith, string matches = null, int start = 0,
                                   int pageSize = Int32.MaxValue, string startAfter = null, string transformer = null, Dictionary<string, object> transformerParameters = null, CancellationToken token = default(CancellationToken))
        {
            var streamOperation = new StreamOperation(this);
            var command = streamOperation.CreateRequest(startsWith, matches, start, pageSize, null, startAfter, transformer,
                transformerParameters);
            await RequestExecutor.ExecuteAsync(command, Context, token, sessionId: _clientSessionId).ConfigureAwait(false);
            var result = streamOperation.SetResultAsync(command.Result);
            return new YieldStream<T>(this, null, null, command.UsedTransformer, result, token);
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