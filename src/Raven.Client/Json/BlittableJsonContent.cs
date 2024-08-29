using System;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Client.Json
{
    internal class BlittableJsonContent : HttpContent
    {
        private static readonly TaskCompletionSource<object> Sentinel = new(TaskCreationOptions.RunContinuationsAsynchronously);

        private TaskCompletionSource<object> _tcs;

        private readonly Func<Stream, Task> _asyncTaskWriter;

        public BlittableJsonContent(Func<Stream, Task> writer)
        {
            _asyncTaskWriter = writer ?? throw new ArgumentNullException(nameof(writer));
            Headers.ContentEncoding.Add("gzip");
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

#if NETSTANDARD2_0 || NETCOREAPP2_1
                using (var gzipStream = new GZipStream(stream, CompressionMode.Compress, leaveOpen: true))
#else
                await using (var gzipStream = new GZipStream(stream, CompressionMode.Compress, leaveOpen: true))
#endif
                {
                    await _asyncTaskWriter(gzipStream).ConfigureAwait(false);
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
