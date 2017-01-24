using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Http;
using Raven.NewClient.Client.Blittable;
using Raven.NewClient.Client.Connection;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Json;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Raven.NewClient.Client.Http;

namespace Raven.NewClient.Client.Commands
{
    public class MultiGetCommand : RavenCommand<BlittableArrayResult>
    {
        /// <summary>
        /// Used for nameof
        /// </summary>
        internal class Response
        {
            public HttpStatusCode StatusCode { get; set; }

            public object Result { get; set; }

            public Dictionary<string, object> Headers { get; set; }
        }

        private readonly JsonOperationContext _context;
        private readonly HttpCache _cache;
        private readonly List<GetRequest> _commands;

        private string _baseUrl;

        public MultiGetCommand(JsonOperationContext context, HttpCache cache, List<GetRequest> commands)
        {
            _context = context;
            _cache = cache;
            _commands = commands;
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
                string requestUrl;
                var cacheKey = GetCacheKey(command, out requestUrl);

                long cachedEtag;
                BlittableJsonReaderObject cachedResponse;
                using (_cache.Get(_context, cacheKey, out cachedEtag, out cachedResponse))
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
                    writer.WriteStartArray();
                    var first = true;
                    foreach (var command in commands)
                    {
                        if (first == false)
                            writer.WriteComma();
                        first = false;
                        _context.Write(writer, command);
                    }
                    writer.WriteEndArray();
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

        public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                ThrowInvalidResponse();

            BlittableJsonReaderArray array;
            if (response.TryGet("Results", out array) == false || array == null)
                ThrowInvalidResponse();

            var anyModifications = false;
            for (int i = 0; i < array.Length; i++)
            {
                var result = (BlittableJsonReaderObject)array[i];

                HttpStatusCode statusCode;
                if (result.TryGet(nameof(Response.StatusCode), out statusCode) == false)
                    continue;

                if (statusCode != HttpStatusCode.NotModified)
                    continue;

                var command = _commands[i];

                string requestUrl;
                var cacheKey = GetCacheKey(command, out requestUrl);

                long cachedEtag;
                BlittableJsonReaderObject cachedResponse;
                using (_cache.Get(_context, cacheKey, out cachedEtag, out cachedResponse))
                {
                    if (result.Modifications == null)
                        result.Modifications = new DynamicJsonValue(result);

                    result.Modifications[nameof(Response.Result)] = cachedResponse;
                    anyModifications = true;
                }
            }

            if (anyModifications)
                response = _context.ReadObject(response, "multi_get/response");

            Result = JsonDeserializationClient.BlittableArrayResult(response);
        }

        protected override void CacheResponse(HttpCache cache, RequestExecuterOptions options, string url, HttpResponseMessage response, BlittableJsonReaderObject responseJson)
        {
            if (_baseUrl == null || responseJson == null)
                return;

            BlittableJsonReaderArray array;
            if (responseJson.TryGet("Results", out array) == false || array == null)
                return;

            for (var i = 0; i < array.Length; i++)
            {
                var result = (BlittableJsonReaderObject)array[i];
                var command = _commands[i];

                string requestUrl;
                var cacheKey = GetCacheKey(command, out requestUrl);

                if (options.ShouldCacheRequest(requestUrl) == false)
                    continue;

                HttpStatusCode statusCode;
                if (result.TryGet(nameof(Response.StatusCode), out statusCode) == false)
                    continue;

                if (statusCode == HttpStatusCode.NotModified)
                    continue;

                BlittableJsonReaderObject responseResult;
                if (result.TryGet(nameof(Response.Result), out responseResult) == false)
                    continue;

                BlittableJsonReaderObject headers;
                if (result.TryGet(nameof(Response.Headers), out headers) == false)
                    continue;

                var etag = headers.GetEtagHeader();
                if (etag.HasValue == false)
                    continue;

                using (var memoryStream = new MemoryStream()) // how to do it better?
                {
                    responseResult.WriteJsonTo(memoryStream);
                    memoryStream.Position = 0;

                    _cache.Set(cacheKey, etag.Value, _context.ReadForMemory(memoryStream, "multi_get/result"));
                }
            }
        }

        public override bool IsReadRequest => false;
    }
}