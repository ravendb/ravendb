using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Queries;
using Raven.Client.Util;
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

        public QueryStreamCommand CreateRequest(string indexName, IndexQuery query)
        {
            _isQueryStream = true;

            if (query.WaitForNonStaleResults || query.WaitForNonStaleResultsAsOfNow)
                throw new NotSupportedException(
                    "Since Stream() does not wait for indexing (by design), streaming query with WaitForNonStaleResults is not supported.");

            if (string.IsNullOrEmpty(indexName))
                throw new ArgumentException("Key cannot be null or empty index");

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
            var state = new JsonParserState();
            JsonOperationContext.ManagedPinnedBuffer buffer;

            using (response.Response)
            using (response.Stream)
            using (var parser = new UnmanagedJsonParser(_session.Context, state, "stream contents"))
            using (_session.Context.GetManagedBuffer(out buffer))
            {
                if (UnmanagedJsonParserHelper.Read(response.Stream, parser, state, buffer) == false)
                    UnmanagedJsonParserHelper.ThrowInvalidJson();

                if (state.CurrentTokenType != JsonParserToken.StartObject)
                    UnmanagedJsonParserHelper.ThrowInvalidJson();


                if(_isQueryStream)
                    HandleStreamQueryStats(_session.Context, response, parser, state, buffer, _statistics);

                var property = UnmanagedJsonParserHelper.ReadString(_session.Context, response.Stream, parser, state, buffer);
                if (string.Equals(property, "Results") == false)
                    UnmanagedJsonParserHelper.ThrowInvalidJson();
                
                foreach (var result in UnmanagedJsonParserHelper.ReadArrayToMemory(_session.Context, response.Stream, parser, state, buffer))
                    yield return result;

                if (UnmanagedJsonParserHelper.Read(response.Stream, parser, state, buffer) == false)
                    UnmanagedJsonParserHelper.ThrowInvalidJson();

                if (state.CurrentTokenType != JsonParserToken.EndObject)
                    UnmanagedJsonParserHelper.ThrowInvalidJson();
            }
        }

        private static void HandleStreamQueryStats(JsonOperationContext context ,StreamResult response, UnmanagedJsonParser parser, JsonParserState state, JsonOperationContext.ManagedPinnedBuffer buffer, StreamQueryStatistics streamQueryStatistics = null)
        {
            var property = UnmanagedJsonParserHelper.ReadString(context, response.Stream, parser, state, buffer);
            if (string.Equals(property, nameof(StreamQueryStatistics.ResultEtag)) == false)
                UnmanagedJsonParserHelper.ThrowInvalidJson();
            var resultEtag =  UnmanagedJsonParserHelper.ReadLong(context, response.Stream, parser, state, buffer);
             
            property = UnmanagedJsonParserHelper.ReadString(context, response.Stream, parser, state, buffer);
            if (string.Equals(property, nameof(StreamQueryStatistics.IsStale)) == false)
                UnmanagedJsonParserHelper.ThrowInvalidJson();

            if (UnmanagedJsonParserHelper.Read(response.Stream, parser, state, buffer) == false)
                UnmanagedJsonParserHelper.ThrowInvalidJson();

            if (state.CurrentTokenType != JsonParserToken.False && state.CurrentTokenType != JsonParserToken.True)
                UnmanagedJsonParserHelper.ThrowInvalidJson();
            var isStale = state.CurrentTokenType != JsonParserToken.False;

            property = UnmanagedJsonParserHelper.ReadString(context, response.Stream, parser, state, buffer);
            if (string.Equals(property, nameof(StreamQueryStatistics.IndexName)) == false)
                UnmanagedJsonParserHelper.ThrowInvalidJson();
            var indexName = UnmanagedJsonParserHelper.ReadString(context, response.Stream, parser, state, buffer);

            property = UnmanagedJsonParserHelper.ReadString(context, response.Stream, parser, state, buffer);
            if (string.Equals(property, nameof(StreamQueryStatistics.TotalResults)) == false)
                UnmanagedJsonParserHelper.ThrowInvalidJson();
            var totalResults = (int)UnmanagedJsonParserHelper.ReadLong(context, response.Stream, parser, state, buffer);

            property = UnmanagedJsonParserHelper.ReadString(context, response.Stream, parser, state, buffer);
            if (string.Equals(property, nameof(StreamQueryStatistics.IndexTimestamp)) == false)
                UnmanagedJsonParserHelper.ThrowInvalidJson();
            var indexTimestamp = UnmanagedJsonParserHelper.ReadString(context, response.Stream, parser, state, buffer);

            if(streamQueryStatistics == null)
                return;

            streamQueryStatistics.IndexName = indexName;
            streamQueryStatistics.IsStale = isStale;
            streamQueryStatistics.TotalResults = totalResults;
            streamQueryStatistics.ResultEtag = resultEtag;

            DateTime timeStamp;
            if (DateTime.TryParseExact(indexTimestamp, "o", CultureInfo.InvariantCulture,
                DateTimeStyles.RoundtripKind, out timeStamp) == false)
                UnmanagedJsonParserHelper.ThrowInvalidJson();
            streamQueryStatistics.IndexTimestamp = timeStamp;
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

            public void Dispose()
            {
                _response.Response.Dispose();
                _response.Stream.Dispose();
                _parser.Dispose();
                _returnBuffer.Dispose();
            }

            public async Task<bool> MoveNextAsync()
            {
                if (_initialized == false)
                    await InitializeAsync().ConfigureAwait(false);

                if (await UnmanagedJsonParserHelper.ReadAsync(_response.Stream, _parser, _state, _buffer).ConfigureAwait(false) == false)
                    UnmanagedJsonParserHelper.ThrowInvalidJson();

                if (_state.CurrentTokenType == JsonParserToken.EndArray)
                {
                    if (await UnmanagedJsonParserHelper.ReadAsync(_response.Stream, _parser, _state, _buffer).ConfigureAwait(false) == false)
                        UnmanagedJsonParserHelper.ThrowInvalidJson();

                    if (_state.CurrentTokenType != JsonParserToken.EndObject)
                        UnmanagedJsonParserHelper.ThrowInvalidJson();

                    return false;
                }

                using (var builder = new BlittableJsonDocumentBuilder(_session.Context, BlittableJsonDocumentBuilder.UsageMode.ToDisk, "readArray/singleResult", _parser, _state))
                {
                    await UnmanagedJsonParserHelper.ReadObjectAsync(builder, _response.Stream, _parser, _buffer).ConfigureAwait(false);

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

                if (await UnmanagedJsonParserHelper.ReadAsync(_response.Stream, _parser, _state, _buffer).ConfigureAwait(false) == false)
                    UnmanagedJsonParserHelper.ThrowInvalidJson();

                if (_state.CurrentTokenType != JsonParserToken.StartObject)
                    UnmanagedJsonParserHelper.ThrowInvalidJson();

                if (_isQueryStream)
                    HandleStreamQueryStats(_session.Context, _response, _parser, _state, _buffer);

                var property = UnmanagedJsonParserHelper.ReadString(_session.Context, _response.Stream, _parser, _state, _buffer);

                if (string.Equals(property, "Results") == false)
                    UnmanagedJsonParserHelper.ThrowInvalidJson();

                if (await UnmanagedJsonParserHelper.ReadAsync(_response.Stream, _parser, _state, _buffer).ConfigureAwait(false) == false)
                    UnmanagedJsonParserHelper.ThrowInvalidJson();

                if (_state.CurrentTokenType != JsonParserToken.StartArray)
                    UnmanagedJsonParserHelper.ThrowInvalidJson();
            }

            public BlittableJsonReaderObject Current { get; private set; }
        }
    }
}
