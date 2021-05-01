using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Http;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Commands.MultiGet
{
    public class MultiGetCommand : RavenCommand<List<GetResponse>>, IDisposable
    {
        private readonly RequestExecutor _requestExecutor;
        private readonly HttpCache _httpCache;
        private readonly List<GetRequest> _commands;

        private string _baseUrl;
        private Cached _cached;

        internal bool AggressivelyCached;

        public MultiGetCommand(RequestExecutor requestExecutor, List<GetRequest> commands)
        {
            _requestExecutor = requestExecutor ?? throw new ArgumentNullException(nameof(requestExecutor));
            _httpCache = _requestExecutor.Cache ?? throw new ArgumentNullException(nameof(_requestExecutor.Cache));
            _commands = commands ?? throw new ArgumentNullException(nameof(commands));
            ResponseType = RavenCommandResponseType.Raw;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            _baseUrl = $"{node.Url}/databases/{node.Database}";
            url = $"{_baseUrl}/multi_get";

            if (MaybeReadAllFromCache(ctx, _requestExecutor.AggressiveCaching.Value))
            {
                AggressivelyCached = true;
                return null;// aggressively cached
            }

            var aggressiveCacheOptions = _requestExecutor.AggressiveCaching.Value;
            if (aggressiveCacheOptions != null)
            {
                Result = new List<GetResponse>();
                foreach (var command in _commands)
                {
                    if (command.CanCacheAggressively == false)
                        break;
                    var cacheKey = GetCacheKey(command, out string _);
                    using (var cachedItem = _httpCache.Get(ctx, cacheKey, out _, out var cached))
                    {
                        if (cached == null ||
                            cachedItem.Age > aggressiveCacheOptions.Duration ||
                            aggressiveCacheOptions.Mode == AggressiveCacheMode.TrackChanges && cachedItem.MightHaveBeenModified)
                            break;

                        Result.Add(new GetResponse
                        {
                            Result = cached,
                            StatusCode = HttpStatusCode.NotModified,
                        });
                    }
                }

                if (Result.Count == _commands.Count)
                {
                    AggressivelyCached = true;
                    return null;// aggressively cached
                }
                // not all of it is cached, might as well read it all
                Result = null;
            }

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                Content = new BlittableJsonContent(async stream =>
                {
                    await using (var writer = new AsyncBlittableJsonTextWriter(ctx, stream))
                    {
                        writer.WriteStartObject();

                        var first = true;
                        writer.WritePropertyName("Requests");
                        writer.WriteStartArray();
                        foreach (var command in _commands)
                        {
                            if (first == false)
                                writer.WriteComma();
                            first = false;

                            writer.WriteStartObject();

                            writer.WritePropertyName(nameof(GetRequest.Url));
                            writer.WriteString($"/databases/{node.Database}{command.Url}");
                            writer.WriteComma();

                            writer.WritePropertyName(nameof(GetRequest.Query));
                            writer.WriteString(command.Query);
                            writer.WriteComma();

                            writer.WritePropertyName(nameof(GetRequest.Method));
                            writer.WriteString(command.Method?.Method);
                            writer.WriteComma();

                            writer.WritePropertyName(nameof(GetRequest.Headers));
                            writer.WriteStartObject();

                            var firstInner = true;
                            foreach (var kvp in command.Headers)
                            {
                                if (firstInner == false)
                                    writer.WriteComma();

                                firstInner = false;
                                writer.WritePropertyName(kvp.Key);
                                writer.WriteString(kvp.Value);
                            }

                            writer.WriteEndObject();
                            writer.WriteComma();

                            writer.WritePropertyName(nameof(GetRequest.Content));
                            if (command.Content != null)
                                command.Content.WriteContent(writer, ctx);
                            else
                                writer.WriteNull();

                            writer.WriteEndObject();
                        }
                        writer.WriteEndArray();

                        writer.WriteEndObject();
                    }
                })
            };

            return request;
        }

        private bool MaybeReadAllFromCache(JsonOperationContext ctx, AggressiveCacheOptions options)
        {
            DisposeCache();

            bool readAllFromCache = options != null;
            var trackChanges = readAllFromCache && options.Mode == AggressiveCacheMode.TrackChanges;

            for (int i = 0; i < _commands.Count; i++)
            {
                var command = _commands[i];

                var cacheKey = GetCacheKey(command, out _);
                var cachedItem = _httpCache.Get(ctx, cacheKey, out var changeVector, out var cached);
                if (cached == null)
                {
                    using (cachedItem)
                    {
                        readAllFromCache = false;
                        continue;
                    }
                }

                if (readAllFromCache && (trackChanges && cachedItem.MightHaveBeenModified || cachedItem.Age > options.Duration || command.CanCacheAggressively == false))
                    readAllFromCache = false;

                command.Headers[Constants.Headers.IfNoneMatch] = $"\"{changeVector}\"";
                _cached ??= new Cached(_commands.Count);
                _cached.Values[i] = (cachedItem, cached);
            }

            if (readAllFromCache)
            {
                using (_cached)
                {
                    Result = new List<GetResponse>(_commands.Count);
                    for (int i = 0; i < _commands.Count; i++)
                    {
                        // ReSharper disable once PossibleNullReferenceException
                        var (_, cached) = _cached.Values[i];
                        Result.Add(new GetResponse { Result = cached.Clone(ctx), StatusCode = HttpStatusCode.NotModified });
                    }
                }

                _cached = null;
            }
            return readAllFromCache;
        }

        private string GetCacheKey(GetRequest command, out string requestUrl)
        {
            requestUrl = $"{_baseUrl}{command.UrlAndQuery}";
            return command.Method != null ? $"{command.Method}-{requestUrl}" : requestUrl;
        }

        public override void SetResponseRaw(HttpResponseMessage response, Stream stream, JsonOperationContext context)
        {
                var state = new JsonParserState();
                using (var parser = new UnmanagedJsonParser(context, state, "multi_get/response"))
            using (context.GetMemoryBuffer(out var buffer))
                using (var peepingTomStream = new PeepingTomStream(stream, context))
            using (_cached)
                {
                    if (UnmanagedJsonParserHelper.Read(peepingTomStream, parser, state, buffer) == false)
                        ThrowInvalidJsonResponse(peepingTomStream);

                    if (state.CurrentTokenType != JsonParserToken.StartObject)
                        ThrowInvalidJsonResponse(peepingTomStream);

                    var property = UnmanagedJsonParserHelper.ReadString(context, peepingTomStream, parser, state, buffer);
                    if (property != nameof(BlittableArrayResult.Results))
                        ThrowInvalidJsonResponse(peepingTomStream);

                    var i = 0;
                Result = new List<GetResponse>(_commands.Count);
                    foreach (var getResponse in ReadResponses(context, peepingTomStream, parser, state, buffer))
                    {
                        var command = _commands[i];

                        MaybeSetCache(getResponse, command);

                    Result.Add(_cached != null && getResponse.StatusCode == HttpStatusCode.NotModified
                        ? new GetResponse { Result = _cached.Values[i].Cached.Clone(context), StatusCode = HttpStatusCode.NotModified }
                        : getResponse);

                        i++;
                    }

                    if (UnmanagedJsonParserHelper.Read(peepingTomStream, parser, state, buffer) == false)
                        ThrowInvalidJsonResponse(peepingTomStream);

                    if (state.CurrentTokenType != JsonParserToken.EndObject)
                        ThrowInvalidJsonResponse(peepingTomStream);
                }
            }

        private static IEnumerable<GetResponse> ReadResponses(JsonOperationContext context, PeepingTomStream peepingTomStream, UnmanagedJsonParser parser, JsonParserState state, JsonOperationContext.MemoryBuffer buffer)
        {
            if (UnmanagedJsonParserHelper.Read(peepingTomStream, parser, state, buffer) == false)
                ThrowInvalidJsonResponse(peepingTomStream);

            if (state.CurrentTokenType != JsonParserToken.StartArray)
                ThrowInvalidJsonResponse(peepingTomStream);

            while (true)
            {
                if (UnmanagedJsonParserHelper.Read(peepingTomStream, parser, state, buffer) == false)
                    ThrowInvalidJsonResponse(peepingTomStream);

                if (state.CurrentTokenType == JsonParserToken.EndArray)
                    break;

                yield return ReadResponse(context, peepingTomStream, parser, state, buffer);
            }
        }

        private static unsafe GetResponse ReadResponse(JsonOperationContext context, PeepingTomStream peepingTomStream, UnmanagedJsonParser parser, JsonParserState state, JsonOperationContext.MemoryBuffer buffer)
        {
            if (state.CurrentTokenType != JsonParserToken.StartObject)
                ThrowInvalidJsonResponse(peepingTomStream);

            var getResponse = new GetResponse();
            while (true)
            {
                if (UnmanagedJsonParserHelper.Read(peepingTomStream, parser, state, buffer) == false)
                    ThrowInvalidJsonResponse(peepingTomStream);

                if (state.CurrentTokenType == JsonParserToken.EndObject)
                    break;

                if (state.CurrentTokenType != JsonParserToken.String)
                    ThrowInvalidJsonResponse(peepingTomStream);

                var property = context.AllocateStringValue(null, state.StringBuffer, state.StringSize).ToString();
                switch (property)
                {
                    case nameof(GetResponse.Result):
                        if (UnmanagedJsonParserHelper.Read(peepingTomStream, parser, state, buffer) == false)
                            ThrowInvalidJsonResponse(peepingTomStream);

                        if (state.CurrentTokenType == JsonParserToken.Null)
                            continue;

                        if (state.CurrentTokenType != JsonParserToken.StartObject)
                            ThrowInvalidJsonResponse(peepingTomStream);

                        using (var builder = new BlittableJsonDocumentBuilder(context, BlittableJsonDocumentBuilder.UsageMode.None, "multi_get/result", parser, state))
                        {
                            UnmanagedJsonParserHelper.ReadObject(builder, peepingTomStream, parser, buffer);
                            getResponse.Result = builder.CreateReader();
                        }
                        continue;
                    case nameof(GetResponse.Headers):
                        if (UnmanagedJsonParserHelper.Read(peepingTomStream, parser, state, buffer) == false)
                            ThrowInvalidJsonResponse(peepingTomStream);

                        if (state.CurrentTokenType == JsonParserToken.Null)
                            continue;

                        if (state.CurrentTokenType != JsonParserToken.StartObject)
                            ThrowInvalidJsonResponse(peepingTomStream);

                        using (var builder = new BlittableJsonDocumentBuilder(context, BlittableJsonDocumentBuilder.UsageMode.None, "multi_get/result", parser, state))
                        {
                            UnmanagedJsonParserHelper.ReadObject(builder, peepingTomStream, parser, buffer);
                            using (var headersJson = builder.CreateReader())
                            {
                                foreach (var propertyName in headersJson.GetPropertyNames())
                                    getResponse.Headers[propertyName] = headersJson[propertyName].ToString();
                            }
                        }
                        continue;
                    case nameof(GetResponse.StatusCode):
                        if (UnmanagedJsonParserHelper.Read(peepingTomStream, parser, state, buffer) == false)
                            ThrowInvalidJsonResponse(peepingTomStream);

                        if (state.CurrentTokenType != JsonParserToken.Integer)
                            ThrowInvalidJsonResponse(peepingTomStream);

                        getResponse.StatusCode = (HttpStatusCode)state.Long;
                        continue;
                    default:
                        ThrowInvalidJsonResponse(peepingTomStream);
                        break;
                }
            }

            return getResponse;
        }

        private void MaybeSetCache(GetResponse getResponse, GetRequest command)
        {
            if (getResponse.StatusCode == HttpStatusCode.NotModified)
                return;

            var cacheKey = GetCacheKey(command, out string _);

            var result = getResponse.Result as BlittableJsonReaderObject;
            if (result == null)
                return;

            var changeVector = getResponse.Headers.GetEtagHeader();
            if (changeVector == null)
                return;

            _httpCache.Set(cacheKey, changeVector, result);
        }

        public override bool IsReadRequest => false;

        public void Dispose()
        {
            DisposeCache();
        }

        private void DisposeCache()
        {
            _cached?.Dispose();
            _cached = null;
        }

        private class Cached : IDisposable
        {
            private readonly int _size;
            public (HttpCache.ReleaseCacheItem Release, BlittableJsonReaderObject Cached)[] Values;

            public Cached(int size)
            {
                _size = size;
                Values = ArrayPool<(HttpCache.ReleaseCacheItem, BlittableJsonReaderObject)>.Shared.Rent(size);
    }

            public void Dispose()
            {
                if (Values == null)
                    return;
                for (int i = 0; i < _size; i++)
                {
                    Values[i].Release.Dispose();
}
                ArrayPool<(HttpCache.ReleaseCacheItem, BlittableJsonReaderObject)>.Shared.Return(Values);
                Values = null;
            }
        }
    }
}
