using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Sparrow.Json
{
    public sealed class AsyncBlittableJsonTextWriter : AbstractBlittableJsonTextWriter, IAsyncDisposable
    {
        private MemoryStream _inner;
        private Task _innerFlushTask;

        private MemoryStream _shadowInner;

        private readonly Stream _outputStream;

        public long BufferCapacity => _inner.Capacity;
        public long BufferUsed => _inner.Length;

        public AsyncBlittableJsonTextWriter(JsonOperationContext context, Stream stream) : base(context, context.CheckoutMemoryStream())
        {
            _outputStream = stream ?? throw new ArgumentNullException(nameof(stream));

            if (_stream is not MemoryStream)
                ThrowInvalidTypeException(_stream?.GetType());

            _inner = (MemoryStream)_stream;
        }

        public async ValueTask WriteStreamAsync(Stream stream, CancellationToken token = default)
        {
            var unmanagedMemory = _pinnedBuffer.Memory;

            if (_pos != 0)
            {
                _inner.Write(unmanagedMemory.Memory.Span.Slice(0, _pos));
                _pos = 0;
                _started = true;
            }

            while (true)
            {
                var read = await stream.ReadAsync(unmanagedMemory.Memory, token).ConfigureAwait(false);
                if (read == 0)
                    break;

                _inner.Write(unmanagedMemory.Memory.Span.Slice(0, read));
                _started = true;

                await MaybeFlushAsync(token).ConfigureAwait(false);
            }

            await FlushAsync(token).ConfigureAwait(false);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<int> MaybeFlushAsync(CancellationToken token = default)
        {
            if (_inner.Length * 2 <= _inner.Capacity)
                return new ValueTask<int>(0);

            return FlushAsync(token);
        }

        public async ValueTask<int> FlushAsync(CancellationToken token = default)
        {
            FlushInternal();

            var currentStream = _inner;
            currentStream.TryGetBuffer(out var bytes);

            var bytesCount = bytes.Count;

            if (_outputStream is MemoryStream)
            {
                // We know it is safe to write and flush synchronously.
                _outputStream.Write(bytes.Array, bytes.Offset, bytesCount);
                _outputStream.Flush();
            }
            else
            {
                // We need to flush async, therefore we check if there is any pending flush.
                if (_innerFlushTask != null)
                {
#if NETCOREAPP2_1_OR_GREATER                    
                    if (_innerFlushTask.IsCompletedSuccessfully == false)
#else
                    if (_innerFlushTask.IsCompleted && _innerFlushTask.IsFaulted == false)
#endif
                    {
                        // We need to wait because it didn't finished, since we are running asynchronously.
                        // OR we need to cause an exception therefore we will request the result so the exception gets thrown.
                        await _innerFlushTask.ConfigureAwait(false);
                    }

                    _innerFlushTask = null;
                }

                if (bytesCount != 0)
                {
                    _innerFlushTask = InternalWriteAsync(_outputStream, bytes.Array, bytes.Offset, bytesCount, token);

                    // Therefore, we swap the inner stream with the shadow stream.
                    _inner = _shadowInner ?? _context.CheckoutMemoryStream();
                    _stream = _inner;
                    _shadowInner = currentStream;
                    _started = true;
                }
            }

            _inner.SetLength(0);
            return bytesCount;
        }

        private async Task InternalWriteAsync(Stream stream, byte[] array, int offset, int bytesCount, CancellationToken token)
        {
            await stream.WriteAsync(array, offset, bytesCount, token).ConfigureAwait(false);
            await stream.FlushAsync(token).ConfigureAwait(false);
        }

        public async ValueTask DisposeAsync()
        {
            DisposeInternal();

            await FlushAsync().ConfigureAwait(false);

            if (_innerFlushTask != null)
            {
                await _innerFlushTask.ConfigureAwait(false);
            }

            // We cant flush IF we haven't written anything. We rely on this behavior to detect exceptions. 
            // Therefore, we need to check before flushing that there is something to be flushed.
            if (_started)
            {
                await _outputStream.FlushAsync().ConfigureAwait(false);
            }
            
            if (_shadowInner != null)
            {
                _context.ReturnMemoryStream(_shadowInner);
            }

            _context.ReturnMemoryStream(_inner);
        }

        private void ThrowInvalidTypeException(Type typeOfStream)
        {
            throw new ArgumentException($"Expected stream to be MemoryStream, but got {(typeOfStream == null ? "null" : typeOfStream.ToString())}.");
        }
    }
}
