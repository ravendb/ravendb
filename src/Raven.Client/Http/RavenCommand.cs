using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Extensions;
using Raven.Client.Util;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Utils;

namespace Raven.Client.Http
{
    public abstract class RavenCommand : RavenCommand<object>
    {
        protected RavenCommand()
        {
            ResponseType = RavenCommandResponseType.Empty;
        }

        protected RavenCommand(RavenCommand copy) : base(copy)
        {
            IsReadRequest = copy.IsReadRequest;
        }

        public override bool IsReadRequest { get; } = false;
    }

    public enum ResponseDisposeHandling
    {
        Manually,
        Automatic
    }

    public interface IBroadcast
    {
        IBroadcast PrepareToBroadcast(JsonOperationContext context, DocumentConventions conventions);
    }
    public interface IRaftCommand
    {
        string RaftUniqueRequestId { get; }
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
        public string SelectedNodeTag { get; protected set; }

        internal long FailoverTopologyEtag = -2;

        protected RavenCommand(RavenCommand<TResult> copy)
        {
            CancellationToken = copy.CancellationToken;
            Timeout = copy.Timeout;
            CanCache = copy.CanCache;
            CanCacheAggressively = copy.CanCacheAggressively;
            SelectedNodeTag = copy.SelectedNodeTag;
            ResponseType = copy.ResponseType;
        }

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

            using (var responseStream = await response.Content.ReadAsStreamAsync().ConfigureAwait(false))
            {
                if (ResponseType == RavenCommandResponseType.Object)
                {
                    var contentLength = response.Content.Headers.ContentLength;
                    if (contentLength.HasValue && contentLength == 0)
                        return ResponseDisposeHandling.Automatic;

                    // we intentionally don't dispose the reader here, we'll be using it
                    // in the command, any associated memory will be released on context reset
                    using (var stream = new StreamReaderWithTimeout(responseStream))
                    {
                        var json = await context.ReadForMemoryAsync(stream, "response/object").ConfigureAwait(false);
                        if (cache != null) //precaution
                        {
                            CacheResponse(cache, url, response, json);
                        }
                        SetResponse(context, json, fromCache: false);
                        return ResponseDisposeHandling.Automatic;
                    }
                }

                // We do not cache the stream response.
                using(var uncompressedStream = await RequestExecutor.ReadAsStreamUncompressedAsync(response).ConfigureAwait(false))
                using(var stream = new StreamReaderWithTimeout(uncompressedStream))
                    SetResponseRaw(response, stream, context);
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
        internal void SetTimeout(TimeSpan timeout)
        {
            Timeout = timeout;
        }
    }

    public enum RavenCommandResponseType
    {
        Empty,
        Object,
        Raw
    }

    public class StreamReaderWithTimeout : Stream, IDisposable
    {
        private static readonly TimeSpan DefaultReadTimeout = TimeSpan.FromSeconds(60);

        private readonly Stream _stream;
        private readonly int _readTimeout;
        private readonly bool _canBaseStreamTimeoutOnRead;
        private CancellationTokenSource _cts;
        public StreamReaderWithTimeout(Stream stream, TimeSpan? readTimeout = null)
        {
            _stream = stream;
            _canBaseStreamTimeoutOnRead = _stream.CanTimeout && _stream.ReadTimeout < int.MaxValue;

            if (_canBaseStreamTimeoutOnRead)
                _readTimeout = _stream.ReadTimeout;
            else
                _readTimeout = (int)(readTimeout ?? DefaultReadTimeout).TotalMilliseconds;
        }

        public override int ReadTimeout => _readTimeout;

        public override bool CanTimeout => true;

        public override void Flush()
        {
            _stream.Flush();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (_canBaseStreamTimeoutOnRead)
                return _stream.Read(buffer, offset, count);

            return AsyncHelpers.RunSync(() => ReadAsyncWithTimeout(buffer, offset, count, CancellationToken.None));
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            if (_canBaseStreamTimeoutOnRead)
                return _stream.ReadAsync(buffer, offset, count, cancellationToken);

            return ReadAsyncWithTimeout(buffer, offset, count, cancellationToken);
        }

        private Task<int> ReadAsyncWithTimeout(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            if (_cts == null)
                _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

            _cts.Token.ThrowIfCancellationRequested();

            return _stream.ReadAsync(buffer, offset, count, _cts.Token).WaitForTaskCompletion(TimeSpan.FromMilliseconds(_readTimeout), _cts);
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            return _stream.Seek(offset, origin);
        }

        public override void SetLength(long value)
        {
            _stream.SetLength(value);
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }

        public override bool CanRead => _stream.CanRead;
        public override bool CanSeek => _stream.CanSeek;
        public override bool CanWrite => false;
        public override long Length => _stream.Length;
        public override long Position
        {
            get => _stream.Position;
            set => _stream.Position = value;
        }

        public new void Dispose()
        {
            base.Dispose(true);
            _stream.Dispose();
            _cts?.Dispose();
        }
    }
}
