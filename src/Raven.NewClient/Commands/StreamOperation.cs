using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using Raven.NewClient.Abstractions.Extensions;
using Raven.NewClient.Abstractions.Util;
using Raven.NewClient.Client.Document;
using Sparrow.Logging;
using Raven.NewClient.Client.Connection;
using Raven.NewClient.Client.Data.Queries;
using Raven.NewClient.Client.Linq;
using Sparrow.Json;
using Sparrow.Json.Parsing;

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

            return new StreamCommand()
            {
                Index = path,
            };
        }

        private static void ReadNextToken(Stream stream, UnmanagedJsonParser parser, JsonOperationContext.ManagedPinnedBuffer buffer)
        {
            while (parser.Read() == false)
            {
                var read = stream.Read(buffer.Buffer.Array, buffer.Buffer.Offset, buffer.Buffer.Count);
                if (read == 0)
                    throw new EndOfStreamException("The stream ended unexpectedly");
                parser.SetBuffer(buffer, read);
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
                        parser.SetBuffer(buffer, read);
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

        private unsafe LazyStringValue GetPropertyName(JsonParserState state)
        {
            return new LazyStringValue(null, state.StringBuffer, state.StringSize, _session.Context);
        }
    }
}