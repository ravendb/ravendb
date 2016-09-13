using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Sparrow;
using Voron.Util;

namespace Raven.Server.Utils
{
    public unsafe class ChunkedMmapStream : Stream
    {
        private readonly PtrSize[] _ptrsSizes;
        private readonly int[] _positions;
        private readonly int _maxChunkSize;
        
        private int _index;

        public ChunkedMmapStream(PtrSize[] ptrsSizes, int maxChunkSize)
        {
            Debug.Assert(ptrsSizes.Take(ptrsSizes.Length - 1).All(x => x.Size == maxChunkSize));
            Debug.Assert(ptrsSizes.Last().Size <= maxChunkSize);

            _ptrsSizes = ptrsSizes;
            _maxChunkSize = maxChunkSize;
            _positions = new int[ptrsSizes.Length];
            
            _index = 0;
            
            foreach (var psp in ptrsSizes)
            {
                Length += psp.Size;
            }
        }

        public override bool CanRead => true;

        public override bool CanSeek => true;

        public override bool CanWrite => false;

        public override void Flush()
        {
            throw new NotSupportedException("The method or operation is not supported by ChunkedMmapStream.");
        }

        public override long Length { get; }

        public override long Position
        {
            get
            {
                return _index * _maxChunkSize + _positions[_index];
            }
            set
            {
                _index = (int) (value / _maxChunkSize);
                
                for (int i = 0; i < _positions.Length; i++)
                {
                    if (i < _index)
                        _positions[i] = _ptrsSizes[i].Size;
                    else if (i > _index)
                        _positions[i] = 0;
                    else
                        _positions[_index] = (int)(value % _maxChunkSize);
                }
            }
        }

        public override int ReadByte()
        {
            var pos = _positions[_index];
            var len = _ptrsSizes[_index].Size;

            if (pos == len)
            {
                if (_index == _ptrsSizes.Length - 1)
                    return -1;

                _index++;
            }

            return _ptrsSizes[_index].Ptr[_positions[_index]++];
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            var pos = _positions[_index];
            var len = _ptrsSizes[_index].Size;

            if (pos == len)
            {
                if (_index == _ptrsSizes.Length - 1)
                    return 0;

                _index++;

                pos = _positions[_index];
                len = _ptrsSizes[_index].Size;
            }

            if (count > len - pos)
                count = len - pos;

            fixed (byte* dst = buffer)
            {
                Memory.CopyInline(dst + offset, _ptrsSizes[_index].Ptr + pos, count);
            }

            _positions[_index] += count;

            return count;
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            switch (origin)
            {
                case SeekOrigin.Begin:
                    Position = offset;
                    break;

                case SeekOrigin.Current:
                    Position += offset;
                    break;

                case SeekOrigin.End:
                    Position = Length + offset;
                    break;
            }

            return Position;
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException("The method or operation is not supported by ChunkedMmapStream.");
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException("The method or operation is not supported by ChunkedMmapStream.");
        }
    }
}