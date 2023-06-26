using System;
using System.Buffers;

namespace Sparrow.Server.Compression
{
    public class AdaptiveMemoryEncoderState : IEncoderState
    {
        private byte[] _encodingBuffer;
        private byte[] _decodingBuffer;
        private int _size;

        public AdaptiveMemoryEncoderState(int size = 16)
        {
            _encodingBuffer = ArrayPool<byte>.Shared.Rent(size);
            _decodingBuffer = ArrayPool<byte>.Shared.Rent(size);
            _size = size;
        }

        public Span<byte> EncodingTable => _encodingBuffer.AsSpan(0, _size);
        public Span<byte> DecodingTable => _decodingBuffer.AsSpan(0, _size);

        public bool CanGrow => true;

        public void Grow(int minimumSize)
        {
            // PERF: The encoder knows the size of the encoder state before starting and the initial size may not be big enough.
            // The implementation of the encoder will call Grow() if the encoder state signals that it can grow to the desired size.
            // Since the encoder hasnt started yet to work, there is nothing of interest into the encoding & decoding buffers,
            // therefore, there is no need to spend time copying the content at the moment of growth. This could change in the
            // future and would require adjusting the implementation of this method.           

            if (_encodingBuffer.Length < minimumSize)
            {
                ArrayPool<byte>.Shared.Return(_encodingBuffer);
                _encodingBuffer = ArrayPool<byte>.Shared.Rent(minimumSize);
            }

            if (_decodingBuffer.Length < minimumSize)
            {
                ArrayPool<byte>.Shared.Return(_decodingBuffer);
                _decodingBuffer = ArrayPool<byte>.Shared.Rent(minimumSize);
            }
  
            _size = minimumSize;
        }
        public void Dispose()
        {
            if (_encodingBuffer != null)
            {
                ArrayPool<byte>.Shared.Return(_encodingBuffer);
                _encodingBuffer = null;
            }

            if (_decodingBuffer != null)
            {
                ArrayPool<byte>.Shared.Return(_decodingBuffer);
                _decodingBuffer = null;
            }
        }
    }
}
