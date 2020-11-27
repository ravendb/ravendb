using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Queries;
using Raven.Client.Util;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Session.Operations
{
    internal class StreamOperation
    {
        private readonly InMemoryDocumentSessionOperations _session;
        private readonly StreamQueryStatistics _statistics;
        private bool _isQueryStream;

        public StreamOperation(InMemoryDocumentSessionOperations session)
        {
            _session = session;
        }

        public StreamOperation(InMemoryDocumentSessionOperations session, StreamQueryStatistics statistics)
        {
            _session = session;
            _statistics = statistics;
        }

        public QueryStreamCommand CreateRequest(IndexQuery query)
        {
            _isQueryStream = true;

            if (query.WaitForNonStaleResults)
                throw new NotSupportedException(
                    "Since Stream() does not wait for indexing (by design), streaming query with WaitForNonStaleResults is not supported.");

            _session.IncrementRequestCount();

            return new QueryStreamCommand(_session.Conventions, query);
        }

        public StreamCommand CreateRequest(string startsWith, string matches, int start, int pageSize, string exclude, string startAfter = null)
        {
            var sb = new StringBuilder("streams/docs?");

            if (startsWith != null)
            {
                sb.Append("startsWith=").Append(Uri.EscapeDataString(startsWith)).Append("&");
            }
            if (matches != null)
            {
                sb.Append("matches=").Append(Uri.EscapeDataString(matches)).Append("&");
            }
            if (exclude != null)
            {
                sb.Append("exclude=").Append(Uri.EscapeDataString(exclude)).Append("&");
            }
            if (startAfter != null)
            {
                sb.Append("startAfter=").Append(Uri.EscapeDataString(startAfter)).Append("&");
            }

            if (start != 0)
                sb.Append("start=").Append(start).Append("&");

            if (pageSize != int.MaxValue)
                sb.Append("pageSize=").Append(pageSize).Append("&");

            return new StreamCommand(sb.ToString());
        }

        public IEnumerator<BlittableJsonReaderObject> SetResult(StreamResult response)
        {
            var enumerator = new YieldStreamResults(_session, response, _isQueryStream, isAsync: false, _statistics);
            enumerator.Initialize();

            return enumerator;
        }

        public async Task<IAsyncEnumerator<BlittableJsonReaderObject>> SetResultAsync(StreamResult response)
        {
            var enumerator = new YieldStreamResults(_session, response, _isQueryStream, isAsync: true, _statistics);
            await enumerator.InitializeAsync().ConfigureAwait(false);

            return enumerator;
        }

        private class YieldStreamResults : IAsyncEnumerator<BlittableJsonReaderObject>, IEnumerator<BlittableJsonReaderObject>
        {
            public YieldStreamResults(InMemoryDocumentSessionOperations session, StreamResult response, bool isQueryStream, bool isAsync, StreamQueryStatistics streamQueryStatistics)
            {
                if (response == null)
                    throw new InvalidOperationException("The index does not exists, failed to stream results");

                _response = response;
                _peepingTomStream = new PeepingTomStream(_response.Stream, session.Context);
                _session = session;
                _isQueryStream = isQueryStream;
                _isAsync = isAsync;
                _streamQueryStatistics = streamQueryStatistics;
                _maxDocsCountOnCachedRenewSession = session._maxDocsCountOnCachedRenewSession;
            }

            private readonly StreamResult _response;
            private readonly InMemoryDocumentSessionOperations _session;
            private readonly int _maxDocsCountOnCachedRenewSession;
            private JsonParserState _state;
            private UnmanagedJsonParser _parser;
            private JsonOperationContext.MemoryBuffer _buffer;
            private bool _initialized;
            private JsonOperationContext.MemoryBuffer.ReturnBuffer _returnBuffer;
            private readonly bool _isQueryStream;
            private readonly bool _isAsync;
            private readonly StreamQueryStatistics _streamQueryStatistics;
            private readonly PeepingTomStream _peepingTomStream;
            private int _docsCountOnCachedRenewSession;
            private bool _cachedItemsRenew;

#if NETSTANDARD2_0 || NETCOREAPP2_1
            public ValueTask DisposeAsync()
#else

            public async ValueTask DisposeAsync()
#endif
            {
#if NETSTANDARD2_0 || NETCOREAPP2_1
                Dispose();
                return default;
#else
                await _response.Stream.DisposeAsync().ConfigureAwait(false);

                DisposeInternal();
#endif
            }

            public void Dispose()
            {
                _response.Stream.Dispose();

                DisposeInternal();
            }

            private void DisposeInternal()
            {
                _response.Response.Dispose();
                _parser.Dispose();
                _returnBuffer.Dispose();
                _peepingTomStream.Dispose();
            }

            public bool MoveNext()
            {
                AssertInitialized();

                CheckIfContextNeedsToBeRenewed();

                if (UnmanagedJsonParserHelper.Read(_peepingTomStream, _parser, _state, _buffer) == false)
                    UnmanagedJsonParserHelper.ThrowInvalidJson(_peepingTomStream);

                if (_state.CurrentTokenType == JsonParserToken.EndArray)
                {
                    if (UnmanagedJsonParserHelper.Read(_peepingTomStream, _parser, _state, _buffer) == false)
                        UnmanagedJsonParserHelper.ThrowInvalidJson(_peepingTomStream);

                    if (_state.CurrentTokenType != JsonParserToken.EndObject)
                        UnmanagedJsonParserHelper.ThrowInvalidJson(_peepingTomStream);

                    return false;
                }

                using (var builder = new BlittableJsonDocumentBuilder(_session.Context, BlittableJsonDocumentBuilder.UsageMode.ToDisk, "readArray/singleResult", _parser, _state))
                {
                    if (_cachedItemsRenew == false)
                        _cachedItemsRenew = builder.NeedResetPropertiesCache();

                    UnmanagedJsonParserHelper.ReadObject(builder, _peepingTomStream, _parser, _buffer);

                    Current = builder.CreateReader();
                    return true;
                }
            }

            public async ValueTask<bool> MoveNextAsync()
            {
                AssertInitialized();

                CheckIfContextNeedsToBeRenewed();

                if (await UnmanagedJsonParserHelper.ReadAsync(_peepingTomStream, _parser, _state, _buffer).ConfigureAwait(false) == false)
                    UnmanagedJsonParserHelper.ThrowInvalidJson(_peepingTomStream);

                if (_state.CurrentTokenType == JsonParserToken.EndArray)
                {
                    if (await UnmanagedJsonParserHelper.ReadAsync(_peepingTomStream, _parser, _state, _buffer).ConfigureAwait(false) == false)
                        UnmanagedJsonParserHelper.ThrowInvalidJson(_peepingTomStream);

                    if (_state.CurrentTokenType != JsonParserToken.EndObject)
                        UnmanagedJsonParserHelper.ThrowInvalidJson(_peepingTomStream);

                    return false;
                }

                using (var builder = new BlittableJsonDocumentBuilder(_session.Context, BlittableJsonDocumentBuilder.UsageMode.ToDisk, "readArray/singleResult", _parser, _state))
                {
                    if (_cachedItemsRenew == false)
                        _cachedItemsRenew = builder.NeedResetPropertiesCache();

                    await UnmanagedJsonParserHelper.ReadObjectAsync(builder, _peepingTomStream, _parser, _buffer).ConfigureAwait(false);

                    Current = builder.CreateReader();
                    return true;
                }
            }

            public void Initialize()
            {
                AssertNotAsync();

                try
                {
                    _initialized = true;

                    _state = new JsonParserState();
                    _parser = new UnmanagedJsonParser(_session.Context, _state, "stream contents");
                    _returnBuffer = _session.Context.GetMemoryBuffer(out _buffer);

                    if (UnmanagedJsonParserHelper.Read(_peepingTomStream, _parser, _state, _buffer) == false)
                        UnmanagedJsonParserHelper.ThrowInvalidJson(_peepingTomStream);

                    if (_state.CurrentTokenType != JsonParserToken.StartObject)
                        UnmanagedJsonParserHelper.ThrowInvalidJson(_peepingTomStream);

                    if (_isQueryStream)
                        HandleStreamQueryStats(_session.Context, _response, _parser, _state, _buffer, _streamQueryStatistics);

                    var property = UnmanagedJsonParserHelper.ReadString(_session.Context, _peepingTomStream, _parser, _state, _buffer);

                    if (string.Equals(property, "Results") == false)
                        UnmanagedJsonParserHelper.ThrowInvalidJson(_peepingTomStream);

                    if (UnmanagedJsonParserHelper.Read(_peepingTomStream, _parser, _state, _buffer) == false)
                        UnmanagedJsonParserHelper.ThrowInvalidJson(_peepingTomStream);

                    if (_state.CurrentTokenType != JsonParserToken.StartArray)
                        UnmanagedJsonParserHelper.ThrowInvalidJson(_peepingTomStream);
                }
                catch
                {
                    Dispose();

                    throw;
                }
            }

            public async Task InitializeAsync()
            {
                AssertNotSync();

                try
                {
                    _initialized = true;

                    _state = new JsonParserState();
                    _parser = new UnmanagedJsonParser(_session.Context, _state, "stream contents");
                    _returnBuffer = _session.Context.GetMemoryBuffer(out _buffer);

                    if (await UnmanagedJsonParserHelper.ReadAsync(_peepingTomStream, _parser, _state, _buffer).ConfigureAwait(false) == false)
                        UnmanagedJsonParserHelper.ThrowInvalidJson(_peepingTomStream);

                    if (_state.CurrentTokenType != JsonParserToken.StartObject)
                        UnmanagedJsonParserHelper.ThrowInvalidJson(_peepingTomStream);

                    if (_isQueryStream)
                        HandleStreamQueryStats(_session.Context, _response, _parser, _state, _buffer, _streamQueryStatistics);

                    var property = UnmanagedJsonParserHelper.ReadString(_session.Context, _peepingTomStream, _parser, _state, _buffer);

                    if (string.Equals(property, "Results") == false)
                        UnmanagedJsonParserHelper.ThrowInvalidJson(_peepingTomStream);

                    if (await UnmanagedJsonParserHelper.ReadAsync(_peepingTomStream, _parser, _state, _buffer).ConfigureAwait(false) == false)
                        UnmanagedJsonParserHelper.ThrowInvalidJson(_peepingTomStream);

                    if (_state.CurrentTokenType != JsonParserToken.StartArray)
                        UnmanagedJsonParserHelper.ThrowInvalidJson(_peepingTomStream);
                }
                catch
                {
                    Dispose();

                    throw;
                }
            }

            public void Reset()
            {
                throw new NotSupportedException("Enumerator does not support resetting");
            }

            public BlittableJsonReaderObject Current { get; private set; }

            object IEnumerator.Current => Current;

            private static void HandleStreamQueryStats(JsonOperationContext context, StreamResult response, UnmanagedJsonParser parser, JsonParserState state, JsonOperationContext.MemoryBuffer buffer, StreamQueryStatistics streamQueryStatistics = null)
            {
                using (var peepingTomStream = new PeepingTomStream(response.Stream, context))
                {
                    var property = UnmanagedJsonParserHelper.ReadString(context, peepingTomStream, parser, state, buffer);
                    if (string.Equals(property, nameof(StreamQueryStatistics.ResultEtag)) == false)
                        UnmanagedJsonParserHelper.ThrowInvalidJson(peepingTomStream);
                    var resultEtag = UnmanagedJsonParserHelper.ReadLong(context, peepingTomStream, parser, state, buffer);

                    property = UnmanagedJsonParserHelper.ReadString(context, peepingTomStream, parser, state, buffer);
                    if (string.Equals(property, nameof(StreamQueryStatistics.IsStale)) == false)
                        UnmanagedJsonParserHelper.ThrowInvalidJson(peepingTomStream);

                    if (UnmanagedJsonParserHelper.Read(peepingTomStream, parser, state, buffer) == false)
                        UnmanagedJsonParserHelper.ThrowInvalidJson(peepingTomStream);

                    if (state.CurrentTokenType != JsonParserToken.False && state.CurrentTokenType != JsonParserToken.True)
                        UnmanagedJsonParserHelper.ThrowInvalidJson(peepingTomStream);
                    var isStale = state.CurrentTokenType != JsonParserToken.False;

                    property = UnmanagedJsonParserHelper.ReadString(context, peepingTomStream, parser, state, buffer);
                    if (string.Equals(property, nameof(StreamQueryStatistics.IndexName)) == false)
                        UnmanagedJsonParserHelper.ThrowInvalidJson(peepingTomStream);
                    var indexName = UnmanagedJsonParserHelper.ReadString(context, peepingTomStream, parser, state, buffer);

                    property = UnmanagedJsonParserHelper.ReadString(context, peepingTomStream, parser, state, buffer);
                    if (string.Equals(property, nameof(StreamQueryStatistics.TotalResults)) == false)
                        UnmanagedJsonParserHelper.ThrowInvalidJson(peepingTomStream);
                    var totalResults = (int)UnmanagedJsonParserHelper.ReadLong(context, peepingTomStream, parser, state, buffer);

                    property = UnmanagedJsonParserHelper.ReadString(context, peepingTomStream, parser, state, buffer);
                    if (string.Equals(property, nameof(StreamQueryStatistics.IndexTimestamp)) == false)
                        UnmanagedJsonParserHelper.ThrowInvalidJson(peepingTomStream);
                    var indexTimestamp = UnmanagedJsonParserHelper.ReadString(context, peepingTomStream, parser, state, buffer);

                    if (streamQueryStatistics == null)
                        return;

                    streamQueryStatistics.IndexName = indexName;
                    streamQueryStatistics.IsStale = isStale;
                    streamQueryStatistics.TotalResults = totalResults;
                    streamQueryStatistics.ResultEtag = resultEtag;

                    if (DateTime.TryParseExact(indexTimestamp, "o", CultureInfo.InvariantCulture,
                        DateTimeStyles.RoundtripKind, out DateTime timeStamp) == false)
                        UnmanagedJsonParserHelper.ThrowInvalidJson(peepingTomStream);
                    streamQueryStatistics.IndexTimestamp = timeStamp;
                }
            }

            private void CheckIfContextNeedsToBeRenewed()
            {
                if (_docsCountOnCachedRenewSession <= _maxDocsCountOnCachedRenewSession)
                {
                    if (_cachedItemsRenew)
                    {
                        _session.Context.CachedProperties.Reset();
                        _session.Context.CachedProperties.Renew();

                        ++_docsCountOnCachedRenewSession;
                        _cachedItemsRenew = false;
                    }
                }
                else
                {
                    _session.Context.Renew();
                    _docsCountOnCachedRenewSession = 0;
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private void AssertInitialized()
            {
                if (_initialized == false)
                    throw new InvalidOperationException("Enumerator is not initialized. Please initialize it first.");
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private void AssertNotSync()
            {
                if (_isAsync == false)
                    throw new InvalidOperationException("Cannot use asynchronous operations in synchronous enumerator");
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private void AssertNotAsync()
            {
                if (_isAsync)
                    throw new InvalidOperationException("Cannot use synchronous operations in asynchronous enumerator");
            }
        }
    }
}
