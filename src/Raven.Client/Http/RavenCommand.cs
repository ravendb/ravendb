using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Extensions;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Http
{
    public abstract class RavenCommand : RavenCommand<object>
    {
        protected RavenCommand()
        {
            ResponseType = RavenCommandResponseType.Empty;
        }

        public override bool IsReadRequest { get; } = false;
    }

    public enum ResponseDisposeHandling
    {
        Manually,
        Automatic
    }

    public abstract class RavenCommand<TResult>
    {
        public CancellationToken CancellationToken = CancellationToken.None;
        public Dictionary<ServerNode, Exception> FailedNodes;

        public TResult Result;
        public abstract bool IsReadRequest { get; }

        public HttpStatusCode StatusCode;

        public RavenCommandResponseType ResponseType { get; protected set; }

        public TimeSpan? Timeout { get; protected set; }
        public bool CanCache { get; protected set; }
        public bool CanCacheAggressively { get; protected set; }

        protected RavenCommand()
        {
            ResponseType = RavenCommandResponseType.Object;
            CanCache = true;
            CanCacheAggressively = true;
        }

        public abstract HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url);

        public virtual void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (ResponseType == RavenCommandResponseType.Empty ||
                ResponseType == RavenCommandResponseType.Raw)
                ThrowInvalidResponse();

            throw new InvalidOperationException($"'{GetType()}' command must override the SetResponse method which expects response with the following type: {ResponseType}.");
        }

        public virtual Task<HttpResponseMessage> SendAsync(HttpClient client, HttpRequestMessage request, CancellationToken token)
        {
            // We must use HttpCompletionOption.ResponseHeadersRead otherwise the client will buffer the response
            // and we'll get OutOfMemoryException in huge responses (> 2GB).
            return client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, token);
        }

        public virtual void SetResponseRaw(HttpResponseMessage response, Stream stream, JsonOperationContext context)
        {
            throw new NotSupportedException($"When {nameof(ResponseType)} is set to Raw then please override this method to handle the response.");
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
            return FailedNodes != null && FailedNodes.ContainsKey(node);
        }

        public virtual async Task<ResponseDisposeHandling> ProcessResponse(JsonOperationContext context, HttpCache cache, HttpResponseMessage response, string url)
        {
            if (ResponseType == RavenCommandResponseType.Empty || response.StatusCode == HttpStatusCode.NoContent)
                return ResponseDisposeHandling.Automatic;

            using (var stream = await response.Content.ReadAsStreamAsync().ConfigureAwait(false))
            {
                if (ResponseType == RavenCommandResponseType.Object)
                {
                    var contentLength = response.Content.Headers.ContentLength;
                    if (contentLength.HasValue && contentLength == 0)
                        return ResponseDisposeHandling.Automatic;

                    // we intentionally don't dispose the reader here, we'll be using it
                    // in the command, any associated memory will be released on context reset
                    var json = await context.ReadForMemoryAsync(stream, "response/object").ConfigureAwait(false);
                    if (cache != null) //precaution
                    {
                        CacheResponse(cache, url, response, json);
                    }
                    SetResponse(context, json, fromCache: false);
                    return ResponseDisposeHandling.Automatic;
                }

                // We do not cache the stream response.
                using (var uncompressedStream = await RequestExecutor.ReadAsStreamUncompressedAsync(response).ConfigureAwait(false))
                    SetResponseRaw(response, uncompressedStream, context);
            }
            return ResponseDisposeHandling.Automatic;
        }

        protected void CacheResponse(HttpCache cache, string url, HttpResponseMessage response, BlittableJsonReaderObject responseJson)
        {
            if (CanCache == false)
                return;

            var changeVector = response.GetEtagHeader();
            if (changeVector == null)
                return;

            cache.Set(url, changeVector, responseJson);
        }

        protected static void ThrowInvalidResponse()
        {
            throw new InvalidDataException("Response is invalid.");
        }

        protected static void ThrowInvalidJsonResponse(PeepingTomStream peepingTomStream)
        {
            UnmanagedJsonParserHelper.ThrowInvalidJsonResponse(peepingTomStream);
        }

        protected void AddChangeVectorIfNotNull(string changeVector, HttpRequestMessage request)
        {
#if DEBUG
            if (IsReadRequest)
            {
                if (ResponseType != RavenCommandResponseType.Raw)
                    throw new InvalidOperationException("No need to add the etag for Get requests as the request executor will add it.");

                throw new InvalidOperationException("Stream responses are not cached so not etag should be used.");
            }
#endif

            if (changeVector != null)
                request.Headers.TryAddWithoutValidation("If-Match", $"\"{changeVector}\"");
        }

        public virtual void OnResponseFailure(HttpResponseMessage response)
        {
            
        }
    }

    public enum RavenCommandResponseType
    {
        Empty,
        Object,
        Raw
    }
}
