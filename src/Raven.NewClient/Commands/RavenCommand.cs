using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Raven.NewClient.Client.Connection;
using Raven.NewClient.Client.Http;
using Sparrow.Json;

namespace Raven.NewClient.Client.Commands
{
    public abstract class RavenCommand<TResult>
    {
        public CancellationToken CancellationToken = CancellationToken.None;

        public HashSet<ServerNode> FailedNodes;

        public TResult Result;
        public int AuthenticationRetries;
        public bool IsReadRequest = true;

        public HttpStatusCode StatusCode;

        public abstract HttpRequestMessage CreateRequest(ServerNode node, out string url);
        public abstract void SetResponse(BlittableJsonReaderObject response);

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
            using(response)
            using (var stream = await response.Content.ReadAsStreamAsync())
            {
                // we intentionally don't dispose the reader here, we'll be using it
                // in the command, any associated memory will be released on context reset
                var blittableJsonReaderObject = await context.ReadForMemoryAsync(stream, "PutResult");
                if (response.Headers.ETag != null)
                {
                    long? etag = response.GetEtagHeader();
                    if (etag != null)
                    {
                        cache.Set(url, (long)etag, blittableJsonReaderObject);
                    }
                }
                SetResponse(blittableJsonReaderObject);
            }
    }
    }
}