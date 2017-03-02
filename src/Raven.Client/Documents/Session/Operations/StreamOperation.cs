using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Raven.Client.Util;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Session.Operations
{
    internal class StreamOperation
    {
        private readonly InMemoryDocumentSessionOperations _session;

        public StreamOperation(InMemoryDocumentSessionOperations session)
        {
            _session = session;
        }

        public StreamCommand CreateRequest(IRavenQueryInspector query)
        {
            var ravenQueryInspector = query;
            var indexQuery = ravenQueryInspector.GetIndexQuery(false);
            if (indexQuery.WaitForNonStaleResults || indexQuery.WaitForNonStaleResultsAsOfNow)
                throw new NotSupportedException(
                    "Since Stream() does not wait for indexing (by design), streaming query with WaitForNonStaleResults is not supported.");
            _session.IncrementRequestCount();
            var index = ravenQueryInspector.IndexQueried;
            if (string.IsNullOrEmpty(index))
                throw new ArgumentException("Key cannot be null or empty index");
            string path;
            if (indexQuery.Query != null && indexQuery.Query.Length > _session.Conventions.MaxLengthOfQueryUsingGetUrl)
            {
                path = indexQuery.GetIndexQueryUrl(index, "streams/queries", includePageSizeEvenIfNotExplicitlySet: false, includeQuery: false);
            }
            else
            {
                path = indexQuery.GetIndexQueryUrl(index, "streams/queries", includePageSizeEvenIfNotExplicitlySet: false);
            }

            return new StreamCommand(path, string.IsNullOrWhiteSpace(indexQuery.Transformer) == false);
        }

        public StreamCommand CreateRequest(long? fromEtag, string startsWith, string matches, int start, int pageSize, string exclude, string startAfter = null, string transformer = null, Dictionary<string, object> transformerParameters = null)
        {
            if (fromEtag != null && startsWith != null)
                throw new InvalidOperationException("Either fromEtag or startsWith must be null, you can't specify both");

            var sb = new StringBuilder("streams/docs?");

            if (fromEtag != null)
            {
                sb.Append("etag=").Append(fromEtag).Append("&");
            }
            else
            {
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
            }

            if (string.IsNullOrEmpty(transformer) == false)
                sb.Append("transformer=").Append(Uri.EscapeDataString(transformer)).Append("&");

            if (transformerParameters != null && transformerParameters.Count > 0)
            {
                foreach (var pair in transformerParameters)
                {
                    var parameterName = pair.Key;
                    var parameterValue = pair.Value;

                    sb.AppendFormat("tp-{0}={1}", parameterName, parameterValue).Append("&");
                }
            }

            if (start != 0)
                sb.Append("start=").Append(start).Append("&");

            if (pageSize != int.MaxValue)
                sb.Append("pageSize=").Append(pageSize).Append("&");

            return new StreamCommand(sb.ToString(), string.IsNullOrWhiteSpace(transformer) == false);
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

        public IAsyncEnumerator<BlittableJsonReaderObject> SetResultAsync(StreamResult response)
        {
            return new YieldStreamResults(_session, response);
        }

        private class YieldStreamResults : IAsyncEnumerator<BlittableJsonReaderObject>
        {
            public YieldStreamResults(InMemoryDocumentSessionOperations session, StreamResult stream)
            {
                _response = stream;
                _session = session;
            }

            private readonly StreamResult _response;
            private readonly InMemoryDocumentSessionOperations _session;
            private JsonParserState _state;
            private UnmanagedJsonParser _parser;
            private JsonOperationContext.ManagedPinnedBuffer _buffer;
            private bool _initialized;
            private JsonOperationContext.ReturnBuffer _returnBuffer;

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