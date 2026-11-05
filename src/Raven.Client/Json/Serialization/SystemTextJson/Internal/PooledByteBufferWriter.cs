using System;
using System.Buffers;

namespace Raven.Client.Json.Serialization.SystemTextJson.Internal
{
    internal sealed class PooledByteBufferWriter : IBufferWriter<byte>, IDisposable
    {
        private byte[] _buffer;
        private int _written;

        public PooledByteBufferWriter(int initialCapacity)
        {
            _buffer = ArrayPool<byte>.Shared.Rent(initialCapacity);
        }

        public Span<byte> WrittenSpan => _buffer.AsSpan(0, _written);

        public void Advance(int count)
        {
            if (count < 0 || _written + count > _buffer.Length)
                throw new ArgumentOutOfRangeException(nameof(count));

            _written += count;
        }

        public Memory<byte> GetMemory(int sizeHint = 0)
        {
            EnsureCapacity(sizeHint);
            return _buffer.AsMemory(_written);
        }

        public Span<byte> GetSpan(int sizeHint = 0)
        {
            EnsureCapacity(sizeHint);
            return _buffer.AsSpan(_written);
        }

        public void Write(ReadOnlySpan<byte> bytes)
        {
            bytes.CopyTo(GetSpan(bytes.Length));
            _written += bytes.Length;
        }

        private void EnsureCapacity(int sizeHint)
        {
            if (sizeHint < 1)
                sizeHint = 1;

            if (_buffer.Length - _written >= sizeHint)
                return;

            byte[] larger = ArrayPool<byte>.Shared.Rent(Math.Max(_buffer.Length * 2, _written + sizeHint));
            _buffer.AsSpan(0, _written).CopyTo(larger);
            ArrayPool<byte>.Shared.Return(_buffer);
            _buffer = larger;
        }

        public void Dispose()
        {
            if (_buffer == null)
                return;

            ArrayPool<byte>.Shared.Return(_buffer);
            _buffer = null;
        }
    }
}
