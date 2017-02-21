using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Extensions;
using Sparrow.Json;

namespace Raven.Client.Http
{
    public abstract class RavenCommand<TResult>
    {
        public CancellationToken CancellationToken = CancellationToken.None;

        public HashSet<ServerNode> FailedNodes;

        public TResult Result;
        public int AuthenticationRetries;
        public abstract bool IsReadRequest { get; }
        public HttpStatusCode StatusCode;

        public RavenCommandResponseType ResponseType { get; protected set; } = RavenCommandResponseType.Object;

        public abstract HttpRequestMessage CreateRequest(ServerNode node, out string url);
        public abstract void SetResponse(BlittableJsonReaderObject response, bool fromCache);

        public virtual void SetResponse(BlittableJsonReaderArray response, bool fromCache)
        {
            throw new NotSupportedException($"When {nameof(ResponseType)} is set to Array then please override this method to handle the response.");
        }

        public virtual void SetResponse(Stream stream, string contentType, long etag, bool fromCache)
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

        public bool IsFailedWithNode(ServerNode leaderNode)
        {
            return FailedNodes != null && FailedNodes.Contains(leaderNode);
        }

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
                    CacheResponse(cache, url, response, json);
                    SetResponse(json, fromCache: false);
                    return;
                }

                if (ResponseType == RavenCommandResponseType.Array)
                {
                    var array = await context.ParseArrayToMemoryAsync(stream, "response/array", BlittableJsonDocumentBuilder.UsageMode.None);
                    SetResponse(array.Item1, fromCache: false);
                    return;
                }

                var contentType = response.Content.Headers.ContentType?.ToString();
                // ReSharper disable once PossibleInvalidOperationException
                var etag = response.GetEtagHeader().Value;
                // TODO: How do we make sure to keep the stream open?
                // Should we write to stream that the user give us instead of return a stream?
                SetResponse(stream, contentType, etag, fromCache: false);
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
    }

    public enum RavenCommandResponseType
    {
        Object,
        Array,
        Stream
    }
}