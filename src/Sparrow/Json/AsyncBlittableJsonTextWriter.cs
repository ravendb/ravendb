using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Sparrow.Json
{
    public class AsyncBlittableJsonTextWriter : AbstractBlittableJsonTextWriter, IAsyncDisposable
    {
        private readonly Stream _outputStream;
        private readonly CancellationToken _cancellationToken;

        public AsyncBlittableJsonTextWriter(JsonOperationContext context, Stream stream, CancellationToken cancellationToken = default) : base(context, context.CheckoutMemoryStream())
        {
            _outputStream = stream ?? throw new ArgumentNullException(nameof(stream));
            _cancellationToken = cancellationToken;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<int> MaybeOuterFlushAsync()
        {
            var innerStream = _stream as MemoryStream;
            if (innerStream == null)
                ThrowInvalidTypeException(_stream?.GetType());
            if (innerStream.Length * 2 <= innerStream.Capacity)
                return new ValueTask<int>(0);

            FlushInternal();
            return new ValueTask<int>(OuterFlushAsync());
        }

        public async Task<int> OuterFlushAsync()
        {
            var innerStream = _stream as MemoryStream;
            if (innerStream == null)
                ThrowInvalidTypeException(_stream?.GetType());

            FlushInternal();
            innerStream.TryGetBuffer(out var bytes);
            var bytesCount = bytes.Count;
            if (bytesCount == 0)
                return 0;
            await _outputStream.WriteAsync(bytes.Array, bytes.Offset, bytesCount, _cancellationToken).ConfigureAwait(false);
            innerStream.SetLength(0);
            return bytesCount;
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
        public async ValueTask<int> MaybeFlushAsync(CancellationToken token = default)
        {
            var innerStream = _stream as MemoryStream;
            if (innerStream == null)
                ThrowInvalidTypeException(_stream?.GetType());
            if (innerStream.Length * 2 <= innerStream.Capacity)
                return 0;

            await FlushInternalAsync().ConfigureAwait(false); // this is OK, because inner stream is a MemoryStream
            return await FlushAsync(token).ConfigureAwait(false);
        }

        public async ValueTask<int> FlushAsync(CancellationToken token = default)
        {
            var innerStream = _stream as MemoryStream;
            if (innerStream == null)
                ThrowInvalidTypeException(_stream?.GetType());
            await FlushInternalAsync().ConfigureAwait(false);
            innerStream.TryGetBuffer(out var bytes);
            var bytesCount = bytes.Count;
            if (bytesCount == 0)
                return 0;
            await _outputStream.WriteAsync(bytes.Array, bytes.Offset, bytesCount, token).ConfigureAwait(false);
            innerStream.SetLength(0);
            return bytesCount;
        }

        public async ValueTask DisposeAsync()
        {
            DisposeInternal();

            if (await FlushAsync().ConfigureAwait(false) > 0)
                await _outputStream.FlushAsync().ConfigureAwait(false);

            _context.ReturnMemoryStream((MemoryStream)_stream);
        }

        private void ThrowInvalidTypeException(Type typeOfStream)
        {
            throw new ArgumentException($"Expected stream to be MemoryStream, but got {(typeOfStream == null ? "null" : typeOfStream.ToString())}.");
        }
    }
}
