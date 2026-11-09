using System;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Net.Http;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Sparrow.Utils;

namespace Raven.Client.Json
{
    internal sealed class BlittableJsonContent : HttpContent
    {
        private static readonly TaskCompletionSource<object> Sentinel = new(TaskCreationOptions.RunContinuationsAsynchronously);

        private TaskCompletionSource<object> _tcs;

        private readonly CancellationTokenSource _abortUpload = new();

        private readonly Func<Stream, Task> _asyncTaskWriter;
        private readonly DocumentConventions _conventions;

        public BlittableJsonContent(Func<Stream, Task> writer, DocumentConventions conventions)
        {
            _asyncTaskWriter = writer ?? throw new ArgumentNullException(nameof(writer));
            _conventions = conventions;

            if (_conventions.UseHttpCompression)
                Headers.ContentEncoding.Add(_conventions.HttpCompressionAlgorithm.GetContentEncoding());
        }

        // In some cases a task returned by HttpClient.SendAsync may be completed before the call to the request content's SerializeToStreamAsync completes
        // This method is used to wait for the completion of SerializeToStreamAsync
        // https://github.com/dotnet/runtime/issues/107082
        public Task EnsureCompletedAsync() =>
            Interlocked.CompareExchange(ref _tcs, Sentinel, null) is null
                ? Task.CompletedTask // SerializeToStreamAsync was never called
                : _tcs!.Task;

        // Over HTTP/2 a server can respond before reading the whole body and never read the rest
        internal async Task WaitForRequestBodyAsync(Task requestBodyTask, HttpResponseMessage response, CancellationToken token)
        {
            if (response == null)
            {
                await requestBodyTask.ConfigureAwait(false);
                return;
            }

            Task<HttpContent> bufferResponseContentTask = null;
            try
            {
                if (response.IsSuccessStatusCode == false)
                {
                    bufferResponseContentTask = BufferResponseContentAsync(response.Content);
                    response.Content = await bufferResponseContentTask.WithCancellation(token).ConfigureAwait(false);

                    _abortUpload.Cancel();
                }

                await requestBodyTask.WithCancellation(token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                _abortUpload.Cancel();
                response.Dispose();

                await requestBodyTask.ConfigureAwait(false);

                if (bufferResponseContentTask != null)
                    (await bufferResponseContentTask.ConfigureAwait(false)).Dispose();

#if NETSTANDARD2_0 || NETSTANDARD2_1
                throw new TaskCanceledException(new OperationCanceledException().Message);
#else
                throw new TaskCanceledException(new OperationCanceledException().Message, null, token);
#endif
            }
        }

        private static async Task<HttpContent> BufferResponseContentAsync(HttpContent content)
        {
            var buffer = new MemoryStream();
            ExceptionDispatchInfo error = null;

            try
            {
                using (var stream = await content.ReadAsStreamAsync().ConfigureAwait(false))
                    await stream.CopyToAsync(buffer).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                error = ExceptionDispatchInfo.Capture(e);
            }

            buffer.Position = 0;

            var bufferedContent = new StreamContent(error == null ? buffer : new ThrowAtEndStream(buffer, error));
            foreach (var header in content.Headers)
                bufferedContent.Headers.TryAddWithoutValidation(header.Key, header.Value);

            content.Dispose();

            return bufferedContent;
        }

        protected override async Task SerializeToStreamAsync(Stream stream, TransportContext context)
        {
            if (Interlocked.CompareExchange(ref _tcs, new(TaskCreationOptions.RunContinuationsAsynchronously), null) is not null)
                throw new InvalidOperationException($"Already called previously, or called after {nameof(EnsureCompletedAsync)}");

            stream = new AbortableWriteStream(stream, _abortUpload.Token);

            try
            {
                // Immediately flush request stream to send headers
                // https://github.com/dotnet/corefx/issues/39586#issuecomment-516210081
                // https://github.com/dotnet/runtime/issues/96223#issuecomment-1865009861
                await stream.FlushAsync().ConfigureAwait(false);

                if (_conventions.UseHttpCompression == false)
                {
                    await _asyncTaskWriter(stream).ConfigureAwait(false);
                    return;
                }

                switch (_conventions.HttpCompressionAlgorithm)
                {
                    case HttpCompressionAlgorithm.Gzip:
#if NETSTANDARD2_0
                    using (var gzipStream = new GZipStream(stream, CompressionLevel.Fastest, leaveOpen: true))
#else
                        await using (var gzipStream = new GZipStream(stream, CompressionLevel.Fastest, leaveOpen: true))
#endif
                        {
                            await _asyncTaskWriter(gzipStream).ConfigureAwait(false);
                        }

                        break;
#if FEATURE_BROTLI_SUPPORT
                case HttpCompressionAlgorithm.Brotli:
                    await using (var brotliStream = new BrotliStream(stream, CompressionLevel.Optimal, leaveOpen: true))
                    {
                        await _asyncTaskWriter(brotliStream).ConfigureAwait(false);
                    }
                    break;
#endif
#if FEATURE_ZSTD_SUPPORT
                    case HttpCompressionAlgorithm.Zstd:
                        await using (var zstdStream = ZstdStream.Compress(stream, CompressionLevel.Fastest, leaveOpen: true))
                        {
                            await _asyncTaskWriter(zstdStream).ConfigureAwait(false);
                        }

                        break;
#endif
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }
            finally
            {
                _tcs!.TrySetResult(null);
            }
        }

        protected override bool TryComputeLength(out long length)
        {
            length = -1;
            return false;
        }

        private sealed class AbortableWriteStream : Stream
        {
            private readonly Stream _inner;
            private readonly CancellationToken _abort;

            public AbortableWriteStream(Stream inner, CancellationToken abort)
            {
                _inner = inner;
                _abort = abort;
            }

            public override bool CanRead => false;

            public override bool CanSeek => false;

            public override bool CanWrite => _inner.CanWrite;

            public override long Length => throw new NotSupportedException();

            public override long Position
            {
                get => throw new NotSupportedException();
                set => throw new NotSupportedException();
            }

            public override void Flush() => _inner.FlushAsync(_abort).GetAwaiter().GetResult();

            public override Task FlushAsync(CancellationToken token)
            {
                if (token.CanBeCanceled == false)
                    return _inner.FlushAsync(_abort);

                return FlushLinkedAsync(token);
            }

            private async Task FlushLinkedAsync(CancellationToken token)
            {
                using (var cts = CancellationTokenSource.CreateLinkedTokenSource(token, _abort))
                    await _inner.FlushAsync(cts.Token).ConfigureAwait(false);
            }

            public override void Write(byte[] buffer, int offset, int count) => _inner.WriteAsync(buffer, offset, count, _abort).GetAwaiter().GetResult();

            public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken token)
            {
                if (token.CanBeCanceled == false)
                    return _inner.WriteAsync(buffer, offset, count, _abort);

                return WriteLinkedAsync(buffer, offset, count, token);
            }

            private async Task WriteLinkedAsync(byte[] buffer, int offset, int count, CancellationToken token)
            {
                using (var cts = CancellationTokenSource.CreateLinkedTokenSource(token, _abort))
                    await _inner.WriteAsync(buffer, offset, count, cts.Token).ConfigureAwait(false);
            }

#if !NETSTANDARD2_0
            public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken token = default)
            {
                if (token.CanBeCanceled == false)
                    return _inner.WriteAsync(buffer, _abort);

                return WriteLinkedAsync(buffer, token);
            }

            private async ValueTask WriteLinkedAsync(ReadOnlyMemory<byte> buffer, CancellationToken token)
            {
                using (var cts = CancellationTokenSource.CreateLinkedTokenSource(token, _abort))
                    await _inner.WriteAsync(buffer, cts.Token).ConfigureAwait(false);
            }
#endif

            public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();

            public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

            public override void SetLength(long value) => throw new NotSupportedException();
        }

        private sealed class ThrowAtEndStream : Stream
        {
            private readonly MemoryStream _inner;
            private readonly ExceptionDispatchInfo _error;

            public ThrowAtEndStream(MemoryStream inner, ExceptionDispatchInfo error)
            {
                _inner = inner;
                _error = error;
            }

            public override bool CanRead => true;

            public override bool CanSeek => false;

            public override bool CanWrite => false;

            public override long Length => throw new NotSupportedException();

            public override long Position
            {
                get => throw new NotSupportedException();
                set => throw new NotSupportedException();
            }

            public override int Read(byte[] buffer, int offset, int count)
            {
                var read = _inner.Read(buffer, offset, count);
                if (read == 0 && count > 0)
                    _error.Throw();

                return read;
            }

            public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken token)
            {
                try
                {
                    return Task.FromResult(Read(buffer, offset, count));
                }
                catch (Exception e)
                {
                    return Task.FromException<int>(e);
                }
            }

            protected override void Dispose(bool disposing)
            {
                if (disposing)
                    _inner.Dispose();

                base.Dispose(disposing);
            }

            public override void Flush() => throw new NotSupportedException();

            public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

            public override void SetLength(long value) => throw new NotSupportedException();

            public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        }
    }
}
