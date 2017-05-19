using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Http;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Commands.MultiGet
{
    public class MultiGetCommand : RavenCommand<List<GetResponse>>
    {
        private readonly JsonOperationContext _context;
        private readonly HttpCache _cache;
        private readonly List<GetRequest> _commands;

        private string _baseUrl;

        public MultiGetCommand(JsonOperationContext context, HttpCache cache, List<GetRequest> commands)
        {
            _context = context;
            _cache = cache;
            _commands = commands;
            ResponseType = RavenCommandResponseType.Raw;
        }

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            _baseUrl = $"{node.Url}/databases/{node.Database}";

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Post
            };

            var commands = new List<DynamicJsonValue>();
            foreach (var command in _commands)
            {
                var cacheKey = GetCacheKey(command, out string _);
                using (_cache.Get(_context, cacheKey, out long cachedEtag, out BlittableJsonReaderObject _))
                {
                    var headers = new DynamicJsonValue();
                    if (cachedEtag != 0)
                        headers["If-None-Match"] = $"\"{cachedEtag}\"";

                    foreach (var header in command.Headers)
                        headers[header.Key] = header.Value;
                    commands.Add(new DynamicJsonValue
                    {
                        [nameof(GetRequest.Url)] = $"/databases/{node.Database}{command.Url}",
                        [nameof(GetRequest.Query)] = $"{command.Query}",
                        [nameof(GetRequest.Method)] = command.Method,
                        [nameof(GetRequest.Headers)] = headers,
                        [nameof(GetRequest.Content)] = command.Content
                    });
                }
            }

            request.Content = new BlittableJsonContent(stream =>
            {
                using (var writer = new BlittableJsonTextWriter(_context, stream))
                {
                    writer.WriteStartObject();
                    writer.WriteArray("Requests", commands, _context);
                    writer.WriteEndObject();
                }
            });

            url = $"{_baseUrl}/multi_get";

            return request;
        }

        private string GetCacheKey(GetRequest command, out string requestUrl)
        {
            requestUrl = $"{_baseUrl}{command.UrlAndQuery}";

            return $"{command.Method}-{requestUrl}";
        }

        public override void SetResponseRaw(HttpResponseMessage response, Stream stream, JsonOperationContext context)
        {
            var state = new JsonParserState();
            using (var parser = new UnmanagedJsonParser(context, state, "multi_get/response"))
            using (_context.GetManagedBuffer(out JsonOperationContext.ManagedPinnedBuffer buffer))
            {
                if (UnmanagedJsonParserHelper.Read(stream, parser, state, buffer) == false)
                    ThrowInvalidResponse();

                if (state.CurrentTokenType != JsonParserToken.StartObject)
                    ThrowInvalidResponse();

                var property = UnmanagedJsonParserHelper.ReadString(context, stream, parser, state, buffer);
                if (property != nameof(BlittableArrayResult.Results))
                    ThrowInvalidResponse();

                var i = 0;
                Result = new List<GetResponse>();
                foreach (var getResponse in ReadResponses(context, stream, parser, state, buffer))
                {
                    var command = _commands[i];

                    MaybeSetCache(getResponse, command);
                    MaybeReadFromCache(getResponse, command);

                    Result.Add(getResponse);

                    i++;
                }

                if (UnmanagedJsonParserHelper.Read(stream, parser, state, buffer) == false)
                    ThrowInvalidResponse();

                if (state.CurrentTokenType != JsonParserToken.EndObject)
                    ThrowInvalidResponse();
            }
        }

        private static IEnumerable<GetResponse> ReadResponses(JsonOperationContext context, Stream stream, UnmanagedJsonParser parser, JsonParserState state, JsonOperationContext.ManagedPinnedBuffer buffer)
        {
            if (UnmanagedJsonParserHelper.Read(stream, parser, state, buffer) == false)
                ThrowInvalidResponse();

            if (state.CurrentTokenType != JsonParserToken.StartArray)
                ThrowInvalidResponse();

            while (true)
            {
                if (UnmanagedJsonParserHelper.Read(stream, parser, state, buffer) == false)
                    ThrowInvalidResponse();

                if (state.CurrentTokenType == JsonParserToken.EndArray)
                    break;

                yield return ReadResponse(context, stream, parser, state, buffer);
            }
        }

        private static unsafe GetResponse ReadResponse(JsonOperationContext context, Stream stream, UnmanagedJsonParser parser, JsonParserState state, JsonOperationContext.ManagedPinnedBuffer buffer)
        {
            if (state.CurrentTokenType != JsonParserToken.StartObject)
                ThrowInvalidResponse();

            var getResponse = new GetResponse();
            while (true)
            {
                if (UnmanagedJsonParserHelper.Read(stream, parser, state, buffer) == false)
                    ThrowInvalidResponse();

                if (state.CurrentTokenType == JsonParserToken.EndObject)
                    break;

                if (state.CurrentTokenType != JsonParserToken.String)
                    ThrowInvalidResponse();

                var property = context.AllocateStringValue(null, state.StringBuffer, state.StringSize).ToString();
                switch (property)
                {
                    case nameof(GetResponse.Result):
                        if (UnmanagedJsonParserHelper.Read(stream, parser, state, buffer) == false)
                            ThrowInvalidResponse();

                        if (state.CurrentTokenType == JsonParserToken.Null)
                            continue;

                        if (state.CurrentTokenType != JsonParserToken.StartObject)
                            ThrowInvalidResponse();

                        using (var builder = new BlittableJsonDocumentBuilder(context, BlittableJsonDocumentBuilder.UsageMode.None, "multi_get/result", parser, state))
                        {
                            UnmanagedJsonParserHelper.ReadObject(builder, stream, parser, buffer);
                            getResponse.Result = builder.CreateReader();
                        }
                        continue;
                    case nameof(GetResponse.Headers):
                        if (UnmanagedJsonParserHelper.Read(stream, parser, state, buffer) == false)
                            ThrowInvalidResponse();

                        if (state.CurrentTokenType == JsonParserToken.Null)
                            continue;

                        if (state.CurrentTokenType != JsonParserToken.StartObject)
                            ThrowInvalidResponse();

                        using (var builder = new BlittableJsonDocumentBuilder(context, BlittableJsonDocumentBuilder.UsageMode.None, "multi_get/result", parser, state))
                        {
                            UnmanagedJsonParserHelper.ReadObject(builder, stream, parser, buffer);
                            using (var headersJson = builder.CreateReader())
                            {
                                foreach (var propertyName in headersJson.GetPropertyNames())
                                    getResponse.Headers[propertyName] = headersJson[propertyName].ToString();
                            }
                        }
                        continue;
                    case nameof(GetResponse.StatusCode):
                        if (UnmanagedJsonParserHelper.Read(stream, parser, state, buffer) == false)
                            ThrowInvalidResponse();

                        if (state.CurrentTokenType != JsonParserToken.Integer)
                            ThrowInvalidResponse();

                        getResponse.StatusCode = (HttpStatusCode)state.Long;
                        continue;
                    default:
                        ThrowInvalidResponse();
                        break;
                }
            }

            return getResponse;
        }

        private void MaybeReadFromCache(GetResponse getResponse, GetRequest command)
        {
            if (getResponse.StatusCode != HttpStatusCode.NotModified)
                return;

            var cacheKey = GetCacheKey(command, out string _);
            using (_cache.Get(_context, cacheKey, out long _, out BlittableJsonReaderObject cachedResponse))
            {
                getResponse.Result = cachedResponse;
            }
        }

        private void MaybeSetCache(GetResponse getResponse, GetRequest command)
        {
            if (getResponse.StatusCode == HttpStatusCode.NotModified)
                return;

            var cacheKey = GetCacheKey(command, out string _);

            var result = getResponse.Result as BlittableJsonReaderObject;
            if (result == null)
                return;

            var etag = getResponse.Headers.GetEtagHeader();
            if (etag.HasValue == false)
                return;

            _cache.Set(cacheKey, etag.Value, result);
        }

        public override bool IsReadRequest => false;
    }
}