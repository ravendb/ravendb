using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Utils;

namespace Sparrow.Json
{
    public sealed class AsyncBlittableJsonTextWriter : AbstractBlittableJsonTextWriter, IAsyncDisposable
    {
        private readonly Stream _outputStream;
        private readonly CancellationToken _cancellationToken;
        
        // PERF: Cache the MemoryStream reference to avoid repeated casting
        private readonly MemoryStream _innerStream;
        private readonly bool _continueOnCapturedContext;

        public AsyncBlittableJsonTextWriter(JsonOperationContext context, Stream stream, CancellationToken cancellationToken = default) : base(context, RecyclableMemoryStreamFactory.GetRecyclableStream())
        {
            _outputStream = stream ?? throw new ArgumentNullException(nameof(stream));
            _cancellationToken = cancellationToken;
            _innerStream = _stream as MemoryStream; // Cache the cast since we know it's always MemoryStream
            _continueOnCapturedContext = AsyncContextHelper.ContinueOnCapturedContext.Value;

            if (_innerStream == null)
                throw new ArgumentException($"Expected stream to be MemoryStream, but got {(_stream?.GetType() == null ? "null" : _stream.ToString())}.");
        }

        #pragma warning disable RDB0002
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
        #pragma warning restore RDB0002

        public bool ShouldFlushAsync
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _innerStream.Length * 2 > _innerStream.Capacity;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<int> MaybeFlushAsync(CancellationToken token = default)
        {
            // PERF: Use cached MemoryStream reference
            if (_innerStream.Length * 2 <= _innerStream.Capacity)
                return new ValueTask<int>(0);

            FlushInternal(); // this is OK, because inner stream is a MemoryStream
            return FlushAsync(token);
        }

        public ValueTask<int> FlushAsync(CancellationToken token = default)
        {
            // PERF: Use cached MemoryStream reference
            FlushInternal();
            _innerStream.TryGetBuffer(out var bytes);
            var bytesCount = bytes.Count;
            if (bytesCount == 0)
                return new ValueTask<int>(0);
            
            var writeTask = _outputStream.WriteAsync(bytes.Array, bytes.Offset, bytesCount, token);
            if (writeTask.IsCompleted)
            {
                // PERF: Fast synchronous path - avoid async state machine overhead
                // This happens when _outputStream is MemoryStream, FileStream with sync completion, etc.
                writeTask.GetAwaiter().GetResult();
                _innerStream.SetLength(0);
                return new ValueTask<int>(bytesCount);
            }
            
            // Slow asynchronous path for network streams, slow disk I/O, etc.
            return FlushAsyncSlow(writeTask, _innerStream, bytesCount);
        }
        
        private async ValueTask<int> FlushAsyncSlow(Task writeTask, MemoryStream innerStream, int bytesCount)
        {
            #pragma warning disable RDB0002 // ConfigureAwait is intentionally dynamic to support dedicated backup thread pump
            await writeTask.ConfigureAwait(_continueOnCapturedContext);
            #pragma warning restore RDB0002

            innerStream.SetLength(0);
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

        #pragma warning disable RDB0002
        private async ValueTask DisposeAsyncSlow(ValueTask<int> flushTask)
        {
            var bytesWritten = await flushTask.ConfigureAwait(_continueOnCapturedContext);
            if (bytesWritten > 0)
                await _outputStream.FlushAsync(_cancellationToken).ConfigureAwait(_continueOnCapturedContext);

            await DisposeStreamAsync().ConfigureAwait(_continueOnCapturedContext);
        }
        #pragma warning restore RDB0002

        #pragma warning disable RDB0002
        private async ValueTask DisposeAsyncSlow(Task outputFlushTask)
        {
            await outputFlushTask.ConfigureAwait(_continueOnCapturedContext);
            await DisposeStreamAsync().ConfigureAwait(_continueOnCapturedContext);
        }
        #pragma warning restore RDB0002

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
