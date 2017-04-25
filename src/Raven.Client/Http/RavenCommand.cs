using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions;
using Raven.Client.Extensions;
using Sparrow.Json;

namespace Raven.Client.Http
{
    public abstract class RavenCommand<TResult>
    {
        public CancellationToken CancellationToken = CancellationToken.None;
        public Dictionary<ServerNode, ExceptionDispatcher.ExceptionSchema> FailedNodes;

        public TResult Result;
        public int AuthenticationRetries;
        public abstract bool IsReadRequest { get; }
        public HttpStatusCode StatusCode;

        public bool AvoidFailover;        

        public RavenCommandResponseType ResponseType { get; protected set; } = RavenCommandResponseType.Object;

        public TimeSpan? Timeout { get; protected set; }

        public abstract HttpRequestMessage CreateRequest(ServerNode node, out string url);
        public abstract void SetResponse(BlittableJsonReaderObject response, bool fromCache);

        public virtual Task<HttpResponseMessage> SendAsync(HttpClient client, HttpRequestMessage request, CancellationToken token)
        {
            return client.SendAsync(request, token);
        }

        public virtual void SetResponse(BlittableJsonReaderArray response, bool fromCache)
        {
            throw new NotSupportedException($"When {nameof(ResponseType)} is set to Array then please override this method to handle the response.");
        }

        public virtual void SetResponseUncached(HttpResponseMessage response, Stream stream)
        {
            throw new NotSupportedException($"When {nameof(ResponseType)} is set to Stream then please override this method to handle the response.");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected string UrlEncode(string value)
        {
            return WebUtility.UrlEncode(value);
        }

        public static void EnsureIsNotNullOrEmpty(string value, string name)
        {
            if (string.IsNullOrEmpty(value))
                throw new ArgumentException($"{name} cannot be null or empty", name);
        }

        public bool IsFailedWithNode(ServerNode node)
        {
			return FailedNodes != null && FailedNodes.ContainsKey(node);        }

        public virtual async Task ProcessResponse(JsonOperationContext context, HttpCache cache, HttpResponseMessage response, string url)
        {
            if (response.Content.Headers.ContentLength.HasValue && response.Content.Headers.ContentLength == 0)
                return;

            using (response)
            using (var stream = await response.Content.ReadAsStreamAsync())
            {
                if (ResponseType == RavenCommandResponseType.Object)
                {
                    // we intentionally don't dispose the reader here, we'll be using it
                    // in the command, any associated memory will be released on context reset
                    var json = await context.ReadForMemoryAsync(stream, "response/object");
                    if (cache != null) //precaution
                    {
                        CacheResponse(cache, url, response, json);
                    }
                    SetResponse(json, fromCache: false);
                    return;
                }

                if (ResponseType == RavenCommandResponseType.Array)
                {
                    var array = await context.ParseArrayToMemoryAsync(stream, "response/array", BlittableJsonDocumentBuilder.UsageMode.None);
                    // TODO: Either cache also arrays or the better way is to remove all array respones by converting them to objects.
                    SetResponse(array.Item1, fromCache: false);
                    return;
                }

            
                // We do not cache the stream response.
                var uncompressedStream = await RequestExecutor.ReadAsStreamUncompressedAsync(response);
              
                SetResponseUncached(response, uncompressedStream);
            }
        }

        protected virtual void CacheResponse(HttpCache cache, string url, HttpResponseMessage response, BlittableJsonReaderObject responseJson)
        {
            var etag = response.GetEtagHeader();
            if (etag.HasValue == false)
                return;

            cache.Set(url, etag.Value, responseJson);
        }

        protected static void ThrowInvalidResponse()
        {
            throw new InvalidDataException("Response is invalid.");
        }

        protected void AddEtagIfNotNull(long? etag, HttpRequestMessage request)
        {
#if DEBUG
            if (IsReadRequest)
            {
                if (ResponseType != RavenCommandResponseType.Stream)
                    throw new InvalidOperationException("No need to add the etag for Get requests as the request executer will add it.");

                throw new InvalidOperationException("Stream responses are not cached so not etag should be used.");
            }
#endif

            if (etag.HasValue)
                request.Headers.TryAddWithoutValidation("If-Match", $"\"{etag.Value}\"");
        }
    }

    public enum RavenCommandResponseType
    {
        Object,
        Array,
        Stream
    }
}