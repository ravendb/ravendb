using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Raven.NewClient.Abstractions.Util;
using Raven.NewClient.Client.Document;
using Sparrow.Logging;
using Raven.NewClient.Client.Connection;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using System.Text;

namespace Raven.NewClient.Client.Commands
{
    public class StreamOperation
    {
        private readonly InMemoryDocumentSessionOperations _session;
        private static readonly Logger _logger = LoggingSource.Instance.GetLogger<StreamOperation>("Raven.Client");

        public StreamOperation(InMemoryDocumentSessionOperations session)
        {
            _session = session;
        }

        protected void LogStream()
        {
            //TODO
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

        public StreamCommand CreateRequest(long? fromEtag, string startsWith, string matches, int start, int pageSize, string exclude, RavenPagingInformation pagingInformation = null, string skipAfter = null, string transformer = null, Dictionary<string, object> transformerParameters = null)
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
                if (skipAfter != null)
                {
                    sb.Append("skipAfter=").Append(Uri.EscapeDataString(skipAfter)).Append("&");
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

            var actualStart = start;

            var nextPage = pagingInformation != null && pagingInformation.IsForPreviousPage(start, pageSize);
            if (nextPage)
                actualStart = pagingInformation.NextPageStart;

            if (actualStart != 0)
                sb.Append("start=").Append(actualStart).Append("&");

            if (pageSize != int.MaxValue)
                sb.Append("pageSize=").Append(pageSize).Append("&");

            if (nextPage)
                sb.Append("next-page=true").Append("&");

            return new StreamCommand(sb.ToString(), string.IsNullOrWhiteSpace(transformer) == false);
        }

        private static void ReadNextToken(Stream stream, UnmanagedJsonParser parser, JsonOperationContext.ManagedPinnedBuffer buffer)
        {
            while (parser.Read() == false)
            {
                var read = stream.Read(buffer.Buffer.Array, buffer.Buffer.Offset, buffer.Buffer.Count);
                if (read == 0)
                    throw new EndOfStreamException("The stream ended unexpectedly");
                parser.SetBuffer(buffer, 0, read);
            }
        }

        private static async Task ReadNextTokenAsync(Stream stream, UnmanagedJsonParser parser, JsonOperationContext.ManagedPinnedBuffer buffer)
        {
            while (parser.Read() == false)
            {
                var read = await stream.ReadAsync(buffer.Buffer.Array, buffer.Buffer.Offset, buffer.Buffer.Count).ConfigureAwait(false);
                if (read == 0)
                    throw new EndOfStreamException("The stream ended unexpectedly");
                parser.SetBuffer(buffer, 0, read);
            }
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
                ReadNextToken(response.Stream, parser, buffer);

                if (state.CurrentTokenType != JsonParserToken.StartObject)
                {
                    throw new InvalidOperationException("Expected stream to start, but got " +
                                                        state.CurrentTokenType);
                }
                ReadNextToken(response.Stream, parser, buffer);

                if (state.CurrentTokenType != JsonParserToken.String)
                {
                    throw new InvalidOperationException("Expected stream intial property, but got " +
                                                        state.CurrentTokenType);
                }

                // TODO: Need to handle initial properties here from QueryHeaderInformation

                var propery = GetPropertyName(state);
                if (propery.Equals("Results") == false)
                {
                    throw new InvalidOperationException("Expected stream property 'Results' but got " + propery);
                }

                ReadNextToken(response.Stream, parser, buffer);

                if (state.CurrentTokenType != JsonParserToken.StartArray)
                {
                    throw new InvalidOperationException("Expected stream intial property, but got " +
                                                        state.CurrentTokenType);
                }
                ReadNextToken(response.Stream, parser, buffer);
                while (state.CurrentTokenType != JsonParserToken.EndArray)
                {
                    _session.Context.CachedProperties.NewDocument();
                    var builder = new BlittableJsonDocumentBuilder(_session.Context, BlittableJsonDocumentBuilder.UsageMode.ToDisk, "ImportObject", parser, state);
                    builder.ReadNestedObject();
                    while (builder.Read() == false)
                    {
                        var read = response.Stream.Read(buffer.Buffer.Array, buffer.Buffer.Offset, buffer.Length);
                        if (read == 0)
                            throw new EndOfStreamException("Stream ended without reaching end of json content");
                        parser.SetBuffer(buffer, 0, read);
                    }
                    builder.FinalizeDocument();
                    ReadNextToken(response.Stream, parser, buffer);
                    yield return builder.CreateReader();
                }

                ReadNextToken(response.Stream, parser, buffer);

                if (state.CurrentTokenType != JsonParserToken.EndObject)
                {
                    throw new InvalidOperationException("Expected stream closing token, but got " +
                                                        state.CurrentTokenType);
                }
            }
        }

        public IAsyncEnumerator<BlittableJsonReaderObject> SetResultAsync(StreamResult response)
        {
            return new YieldStreamResults(_session, response);
        }

        private unsafe LazyStringValue GetPropertyName(JsonParserState state)
        {
            return new LazyStringValue(null, state.StringBuffer, state.StringSize, _session.Context);
        }

        private class YieldStreamResults : IAsyncEnumerator<BlittableJsonReaderObject>
        {
            private bool complete;

            public YieldStreamResults(InMemoryDocumentSessionOperations session, StreamResult stream)
            {
                _response = stream;
                _session = session;
            }
            private readonly StreamResult _response;
            private readonly InMemoryDocumentSessionOperations _session;
            public void Dispose()
            {
                _response.Response.Dispose();
                _response.Stream.Dispose();
            }

            public async Task<bool> MoveNextAsync()
            {
                if (complete)
                    return false;
                var state = new JsonParserState();
                JsonOperationContext.ManagedPinnedBuffer buffer;

                using (var parser = new UnmanagedJsonParser(_session.Context, state, "stream contents"))
                using (_session.Context.GetManagedBuffer(out buffer))
                {
                    await ReadNextTokenAsync(_response.Stream, parser, buffer).ConfigureAwait(false);

                    if (state.CurrentTokenType != JsonParserToken.StartObject)
                    {
                        throw new InvalidOperationException("Expected stream to start, but got " +
                                                            state.CurrentTokenType);
                    }
                    await ReadNextTokenAsync(_response.Stream, parser, buffer).ConfigureAwait(false);

                    if (state.CurrentTokenType != JsonParserToken.String)
                    {
                        throw new InvalidOperationException("Expected stream intial property, but got " +
                                                            state.CurrentTokenType);
                    }

                    // TODO: Need to handle initial properties here from QueryHeaderInformation

                    var propery = GetPropertyName(state);
                    if (propery.Equals("Results") == false)
                    {
                        throw new InvalidOperationException("Expected stream property 'Results' but got " + propery);
                    }

                    await ReadNextTokenAsync(_response.Stream, parser, buffer).ConfigureAwait(false);

                    if (state.CurrentTokenType != JsonParserToken.StartArray)
                    {
                        throw new InvalidOperationException("Expected stream intial property, but got " +
                                                            state.CurrentTokenType);
                    }
                    await ReadNextTokenAsync(_response.Stream, parser, buffer).ConfigureAwait(false);

                    _session.Context.CachedProperties.NewDocument();
                    var builder = new BlittableJsonDocumentBuilder(_session.Context, BlittableJsonDocumentBuilder.UsageMode.ToDisk, "ImportObject", parser, state);
                    builder.ReadNestedObject();
                    while (builder.Read() == false)
                    {
                        var read = await _response.Stream.ReadAsync(buffer.Buffer.Array, buffer.Buffer.Offset, buffer.Length).ConfigureAwait(false);
                        if (read == 0)
                            throw new EndOfStreamException("Stream ended without reaching end of json content");
                        parser.SetBuffer(buffer, 0, read);
                    }
                    builder.FinalizeDocument();
                    await ReadNextTokenAsync(_response.Stream, parser, buffer).ConfigureAwait(false);
                    Current = builder.CreateReader();

                    if (state.CurrentTokenType == JsonParserToken.EndArray)
                    {
                        await ReadNextTokenAsync(_response.Stream, parser, buffer).ConfigureAwait(false);

                        if (state.CurrentTokenType != JsonParserToken.EndObject)
                        {
                            throw new InvalidOperationException("Expected stream closing token, but got " +
                                                                state.CurrentTokenType);
                        }
                        complete = true;
                        return true;
                    }

                    await ReadNextTokenAsync(_response.Stream, parser, buffer).ConfigureAwait(false);

                    if (state.CurrentTokenType != JsonParserToken.EndObject)
                    {
                        throw new InvalidOperationException("Expected stream closing token, but got " +
                                                            state.CurrentTokenType);
                    }
                    return true;
                }
            }

            public BlittableJsonReaderObject Current { get; private set; }

            private unsafe LazyStringValue GetPropertyName(JsonParserState state)
            {
                return new LazyStringValue(null, state.StringBuffer, state.StringSize, _session.Context);
            }
        }
    }
}