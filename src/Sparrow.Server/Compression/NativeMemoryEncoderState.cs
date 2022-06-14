using System;

namespace Sparrow.Server.Compression
{
    public readonly unsafe struct NativeMemoryEncoderState : IEncoderState
    {
        private readonly byte* _buffer;
        private readonly int _size;

        public NativeMemoryEncoderState(byte* buffer, int size)
        {
            _buffer = buffer;
            _size = size;
        }

        public Span<byte> EncodingTable => new Span<byte>(_buffer, _size/2);
        public Span<byte> DecodingTable => new Span<byte>(_buffer + _size/2, _size/2);

        public bool CanGrow => false;

        public void Grow(int minimumSize)
        {
            throw new NotSupportedException($"{nameof(NativeMemoryEncoderState)} does not support '.{nameof(Grow)}()'.");
        }

        public void Dispose() {}
    }
}
