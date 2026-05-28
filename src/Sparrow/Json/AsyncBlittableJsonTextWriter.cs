using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.IO;

namespace Sparrow.Json
{
    public sealed class AsyncBlittableJsonTextWriter : AbstractBlittableJsonTextWriter, IAsyncDisposable
    {
        private readonly Stream _outputStream;
        private readonly CancellationToken _cancellationToken;
        
        // PERF: Cache the RecyclableMemoryStream reference to avoid repeated casting
        private readonly RecyclableMemoryStream _innerStream;
        private readonly bool _continueOnCapturedContext;

        internal static readonly AsyncLocal<bool> CaptureContextOnAwait = new();

        public AsyncBlittableJsonTextWriter(JsonOperationContext context, Stream stream, CancellationToken cancellationToken = default) : base(context, RecyclableMemoryStreamFactory.GetRecyclableStream())
        {
            _outputStream = stream ?? throw new ArgumentNullException(nameof(stream));
            _cancellationToken = cancellationToken;
            _innerStream = _stream as RecyclableMemoryStream; // Cache the cast since we know it's always RecyclableMemoryStream
            _continueOnCapturedContext = CaptureContextOnAwait.Value;

            if (_innerStream == null)
                throw new ArgumentException($"Expected stream to be RecyclableMemoryStream, but got {(_stream?.GetType() == null ? "null" : _stream.ToString())}.");
        }

        public async ValueTask WriteStreamAsync(Stream stream, CancellationToken token = default)
        {
            await FlushAsync(token).ConfigureAwait(_continueOnCapturedContext);

            while (true)
            {
                _pos = await stream.ReadAsync(_pinnedBuffer.Memory.Memory, token).ConfigureAwait(_continueOnCapturedContext);
                if (_pos == 0)
                    break;

                await FlushAsync(token).ConfigureAwait(_continueOnCapturedContext);
            }
        }

        public bool ShouldFlushAsync
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _innerStream.Length * 2 > _innerStream.Capacity;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<int> MaybeFlushAsync(CancellationToken token = default)
        {
            // PERF: Use cached RecyclableMemoryStream reference
            if (_innerStream.Length * 2 <= _innerStream.Capacity)
                return new ValueTask<int>(0);

            FlushInternal(); // this is OK, because inner stream is a RecyclableMemoryStream
            return FlushAsync(token);
        }

        public ValueTask<int> FlushAsync(CancellationToken token = default)
        {
            FlushInternal();
            var bytesCount = (int)_innerStream.Length;
            if (bytesCount == 0)
                return new ValueTask<int>(0);

            _innerStream.Position = 0;
            // bufferSize is required by the netstandard2.0 overload but is unused by RecyclableMemoryStream.CopyToAsync, which writes each internal block directly.
            var copyTask = _innerStream.CopyToAsync(_outputStream, bufferSize: 4096, token);
            if (copyTask.IsCompleted)
            {
                // PERF: Fast synchronous path - avoid async state machine overhead
                copyTask.GetAwaiter().GetResult();
                _innerStream.SetLength(0);
                return new ValueTask<int>(bytesCount);
            }

            return FlushAsyncSlow(copyTask, bytesCount);
        }

        private async ValueTask<int> FlushAsyncSlow(Task copyTask, int bytesCount)
        {
            await copyTask.ConfigureAwait(_continueOnCapturedContext);

            _innerStream.SetLength(0);
            return bytesCount;
        }

        public ValueTask DisposeAsync()
        {
            DisposeInternal();

            // PERF: Check if flush completed synchronously to avoid async state machine
            var flushTask = FlushAsync(_cancellationToken);
            if (flushTask.IsCompletedSuccessfully)
            {
                // Fast synchronous path
                var bytesWritten = flushTask.Result;
                if (bytesWritten > 0)
                {
                    var outputFlushTask = _outputStream.FlushAsync(_cancellationToken);
                    if (outputFlushTask.IsCompleted)
                    {
                        outputFlushTask.GetAwaiter().GetResult();
                        return DisposeStreamAsync();
                    }
                    else
                    {
                        return DisposeAsyncSlow(outputFlushTask);
                    }
                }
                else
                {
                    return DisposeStreamAsync();
                }
            }
            
            return DisposeAsyncSlow(flushTask);
        }

        private async ValueTask DisposeAsyncSlow(ValueTask<int> flushTask)
        {
            var bytesWritten = await flushTask.ConfigureAwait(_continueOnCapturedContext);
            if (bytesWritten > 0)
                await _outputStream.FlushAsync(_cancellationToken).ConfigureAwait(_continueOnCapturedContext);

            await DisposeStreamAsync().ConfigureAwait(_continueOnCapturedContext);
        }

        private async ValueTask DisposeAsyncSlow(Task outputFlushTask)
        {
            await outputFlushTask.ConfigureAwait(_continueOnCapturedContext);
            await DisposeStreamAsync().ConfigureAwait(_continueOnCapturedContext);
        }

        private ValueTask DisposeStreamAsync()
        {
#if !NETSTANDARD2_0
            return _stream.DisposeAsync();
#else
            _stream.Dispose();
            return default;
#endif
        }

    }
}
