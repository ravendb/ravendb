using System;
using System.Diagnostics;
using System.IO;

namespace Sparrow
{
    public unsafe class PeepingTomStream
    {
        public const int BufferWindowSize = 4096;

        private readonly byte[] _bufferWindow = new byte[BufferWindowSize];
        private int _pos;
        private readonly Stream _stream;
        private bool _firstWindow = true;

        public PeepingTomStream(Stream stream)
        {
            _stream = stream;
        }

        public int Read(byte[] buffer, int offset, int count)
        {
            var read = _stream.Read(buffer, offset, count);
            var totalToRead = read < BufferWindowSize ? read : BufferWindowSize;

            fixed (byte* pDest = _bufferWindow)
            fixed (byte* pSrc = buffer)
            {
                var pBufferWindowStart = pDest + _pos;
                var pBufferStart = pSrc + offset + read - totalToRead;

                if (_pos + totalToRead > BufferWindowSize) // copy in two parts
                {
                    var newTotal = BufferWindowSize - _pos;
                    Memory.Copy(pBufferWindowStart, pBufferStart, newTotal);
                    var nextLength = totalToRead - newTotal;
                    Debug.Assert(nextLength <= BufferWindowSize);
                    Memory.Copy(pDest, pBufferStart + newTotal, nextLength);
                }
                else
                {
                    Memory.Copy(pBufferWindowStart, pBufferStart, totalToRead);
                }
            }

            _pos += totalToRead;
            if (_pos > BufferWindowSize)
            {
                _firstWindow = false;
                _pos %= BufferWindowSize;
            }

            return read;
        }

        public byte[] PeepInReadStream()
        {
            int start,  size;
            if (_firstWindow)
            {
                start = 0;
                size = _pos;
            }
            else
            {
                start = _pos;
                size = BufferWindowSize;
            }
            while ((_bufferWindow[start] & 0x80) != 0)
            {
                start++;
                size--;
                if (start >= BufferWindowSize)
                    start = 0;
            }
            var buf = new byte[size];
            if (size == 0)
                return buf;
            fixed (byte* pDest = buf)
            fixed (byte* pSrc = _bufferWindow)
            {
                var firstSize = BufferWindowSize - start;
                Memory.Copy(pDest, pSrc+ start, firstSize);
                Memory.Copy(pDest + firstSize, pSrc, start);
                return buf;
            }
        }        
    }
}
