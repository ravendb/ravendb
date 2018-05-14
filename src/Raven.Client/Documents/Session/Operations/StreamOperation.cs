using System;
using System.Collections.Generic;
using System.Globalization;
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
            if(response == null)
                throw new InvalidOperationException("The index does not exists, failed to stream results");

            var state = new JsonParserState();
            JsonOperationContext.ManagedPinnedBuffer buffer;

            using (response.Response)
            using (response.Stream)
            using (var parser = new UnmanagedJsonParser(_session.Context, state, "stream contents"))
            using (_session.Context.GetManagedBuffer(out buffer))
            using (var peepingTomStream = new PeepingTomStream(response.Stream, _session.Context))
            {
                if (UnmanagedJsonParserHelper.Read(peepingTomStream, parser, state, buffer) == false)
                    UnmanagedJsonParserHelper.ThrowInvalidJson(peepingTomStream);

                if (state.CurrentTokenType != JsonParserToken.StartObject)
                    UnmanagedJsonParserHelper.ThrowInvalidJson(peepingTomStream);


                if (_isQueryStream)
                    HandleStreamQueryStats(_session.Context, response, parser, state, buffer, _statistics);

                var property = UnmanagedJsonParserHelper.ReadString(_session.Context, peepingTomStream, parser, state, buffer);
                if (string.Equals(property, "Results") == false)
                    UnmanagedJsonParserHelper.ThrowInvalidJson(peepingTomStream);

                foreach (var result in UnmanagedJsonParserHelper.ReadArrayToMemory(_session.Context, peepingTomStream, parser, state, buffer))
                    yield return result;

                if (UnmanagedJsonParserHelper.Read(peepingTomStream, parser, state, buffer) == false)
                    UnmanagedJsonParserHelper.ThrowInvalidJson(peepingTomStream);

                if (state.CurrentTokenType != JsonParserToken.EndObject)
                    UnmanagedJsonParserHelper.ThrowInvalidJson(peepingTomStream);
            }
        }

        private static void HandleStreamQueryStats(JsonOperationContext context, StreamResult response, UnmanagedJsonParser parser, JsonParserState state, JsonOperationContext.ManagedPinnedBuffer buffer, StreamQueryStatistics streamQueryStatistics = null)
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

        public IAsyncEnumerator<BlittableJsonReaderObject> SetResultAsync(StreamResult response)
        {
            return new YieldStreamResults(_session, response, _isQueryStream);
        }

        private class YieldStreamResults : IAsyncEnumerator<BlittableJsonReaderObject>
        {
            public YieldStreamResults(InMemoryDocumentSessionOperations session, StreamResult stream, bool isQueryStream)
            {
                _response = stream;
                _peepingTomStream = new PeepingTomStream(_response.Stream, session.Context);
                _session = session;
                _isQueryStream = isQueryStream;
            }

            private readonly StreamResult _response;
            private readonly InMemoryDocumentSessionOperations _session;
            private JsonParserState _state;
            private UnmanagedJsonParser _parser;
            private JsonOperationContext.ManagedPinnedBuffer _buffer;
            private bool _initialized;
            private JsonOperationContext.ReturnBuffer _returnBuffer;
            private readonly bool _isQueryStream;
            private readonly PeepingTomStream _peepingTomStream;
            private int _docsCountOnCachedRenewSession;
            private bool _cachedItemsRenew;

            public void Dispose()
            {
                _response.Response.Dispose();
                _response.Stream.Dispose();
                _parser.Dispose();
                _returnBuffer.Dispose();
                _peepingTomStream.Dispose();
            }

            public async Task<bool> MoveNextAsync()
            {
                if (_initialized == false)
                    await InitializeAsync().ConfigureAwait(false);

                if (_docsCountOnCachedRenewSession <= 16 * 1024)
                {
                    if (_cachedItemsRenew)
                    {
                        _session.Context.CachedProperties = new CachedProperties(_session.Context);
                        ++_docsCountOnCachedRenewSession;
                    }
                }
                else
                {
                    _session.Context.Renew();
                    _docsCountOnCachedRenewSession = 0;
                }

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

            private async Task InitializeAsync()
            {
                _initialized = true;

                _state = new JsonParserState();
                _parser = new UnmanagedJsonParser(_session.Context, _state, "stream contents");
                _returnBuffer = _session.Context.GetManagedBuffer(out _buffer);

                if (await UnmanagedJsonParserHelper.ReadAsync(_peepingTomStream, _parser, _state, _buffer).ConfigureAwait(false) == false)
                    UnmanagedJsonParserHelper.ThrowInvalidJson(_peepingTomStream);

                if (_state.CurrentTokenType != JsonParserToken.StartObject)
                    UnmanagedJsonParserHelper.ThrowInvalidJson(_peepingTomStream);

                if (_isQueryStream)
                    HandleStreamQueryStats(_session.Context, _response, _parser, _state, _buffer);

                var property = UnmanagedJsonParserHelper.ReadString(_session.Context, _peepingTomStream, _parser, _state, _buffer);

                if (string.Equals(property, "Results") == false)
                    UnmanagedJsonParserHelper.ThrowInvalidJson(_peepingTomStream);

                if (await UnmanagedJsonParserHelper.ReadAsync(_peepingTomStream, _parser, _state, _buffer).ConfigureAwait(false) == false)
                    UnmanagedJsonParserHelper.ThrowInvalidJson(_peepingTomStream);

                if (_state.CurrentTokenType != JsonParserToken.StartArray)
                    UnmanagedJsonParserHelper.ThrowInvalidJson(_peepingTomStream);
            }

            public BlittableJsonReaderObject Current { get; private set; }
        }
    }
}
