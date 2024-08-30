using System;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Sparrow.Utils;

namespace Raven.Client.Json
{
    internal sealed class BlittableJsonContent : HttpContent
    {
        private static readonly TaskCompletionSource<object> Sentinel = new(TaskCreationOptions.RunContinuationsAsynchronously);

        private TaskCompletionSource<object> _tcs;

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

        protected override async Task SerializeToStreamAsync(Stream stream, TransportContext context)
        {
            if (Interlocked.CompareExchange(ref _tcs, new(TaskCreationOptions.RunContinuationsAsynchronously), null) is not null)
                throw new InvalidOperationException($"Already called previously, or called after {nameof(EnsureCompletedAsync)}");

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
    }
}
