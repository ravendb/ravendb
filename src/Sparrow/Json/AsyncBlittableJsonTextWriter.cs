using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Sparrow.Json
{
    public sealed class AsyncBlittableJsonTextWriter : AbstractBlittableJsonTextWriter, IAsyncDisposable
    {
        private readonly Stream _outputStream;
        private readonly CancellationToken _cancellationToken;
        
        // PERF: Cache the MemoryStream reference to avoid repeated casting
        private readonly MemoryStream _memoryStream;

        public AsyncBlittableJsonTextWriter(JsonOperationContext context, Stream stream, CancellationToken cancellationToken = default) : base(context, RecyclableMemoryStreamFactory.GetRecyclableStream())
        {
            _outputStream = stream ?? throw new ArgumentNullException(nameof(stream));
            _cancellationToken = cancellationToken;
            _memoryStream = (MemoryStream)_stream; // Cache the cast since we know it's always MemoryStream
        }

        public async ValueTask WriteStreamAsync(Stream stream, CancellationToken token = default)
        {
            await FlushAsync(token).ConfigureAwait(false);

            while (true)
            {
                _pos = await stream.ReadAsync(_pinnedBuffer.Memory.Memory, token).ConfigureAwait(false);
                if (_pos == 0)
                    break;

                await FlushAsync(token).ConfigureAwait(false);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<int> MaybeFlushAsync(CancellationToken token = default)
        {
            // PERF: Use cached MemoryStream reference
            if (_memoryStream.Length * 2 <= _memoryStream.Capacity)
                return new ValueTask<int>(0);

            FlushInternal(); // this is OK, because inner stream is a MemoryStream
            return FlushAsync(token);
        }

        public ValueTask<int> FlushAsync(CancellationToken token = default)
        {
            // PERF: Use cached MemoryStream reference
            FlushInternal();
            _memoryStream.TryGetBuffer(out var bytes);
            var bytesCount = bytes.Count;
            if (bytesCount == 0)
                return new ValueTask<int>(0);
            
            var writeTask = _outputStream.WriteAsync(bytes.Array, bytes.Offset, bytesCount, token);
            if (writeTask.IsCompleted)
            {
                // PERF: Fast synchronous path - avoid async state machine overhead
                // This happens when _outputStream is MemoryStream, FileStream with sync completion, etc.
                writeTask.GetAwaiter().GetResult();
                _memoryStream.SetLength(0);
                return new ValueTask<int>(bytesCount);
            }
            
            // Slow asynchronous path for network streams, slow disk I/O, etc.
            return FlushAsyncSlow(writeTask, _memoryStream, bytesCount);
        }
        
        private static async ValueTask<int> FlushAsyncSlow(Task writeTask, MemoryStream innerStream, int bytesCount)
        {
            await writeTask.ConfigureAwait(false);
            innerStream.SetLength(0);
            return bytesCount;
        }

        public ValueTask DisposeAsync()
        {
            DisposeInternal();

            // PERF: Check if flush completed synchronously to avoid async state machine
            var flushTask = FlushAsync();
            if (flushTask.IsCompleted)
            {
                // Fast synchronous path
                var bytesWritten = flushTask.GetAwaiter().GetResult();
                if (bytesWritten > 0)
                {
                    var outputFlushTask = _outputStream.FlushAsync();
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
            var bytesWritten = await flushTask.ConfigureAwait(false);
            if (bytesWritten > 0)
                await _outputStream.FlushAsync().ConfigureAwait(false);

            await DisposeStreamAsync().ConfigureAwait(false);
        }

        private async ValueTask DisposeAsyncSlow(Task outputFlushTask)
        {
            await outputFlushTask.ConfigureAwait(false);
            await DisposeStreamAsync().ConfigureAwait(false);
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
