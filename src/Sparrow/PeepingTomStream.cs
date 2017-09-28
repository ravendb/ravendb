using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using Sparrow.Json;
using static Sparrow.Json.JsonOperationContext;

namespace Sparrow
{
    public class PeepingTomStream : IDisposable
    {
        public const int BufferWindowSize = 4096;

        private readonly ManagedPinnedBuffer _bufferWindow;
        private int _pos;
        private readonly Stream _stream;
        private bool _firstWindow = true;
        private ReturnBuffer _returnedBuffer;

        public PeepingTomStream(Stream stream, JsonOperationContext context)
        {
            _stream = stream;
            _returnedBuffer = context.GetManagedBuffer(out _bufferWindow);
        }

        public int Read(byte[] buffer, int offset, int count)
        {
            var read = _stream.Read(buffer, offset, count);
            return ReadInternal(buffer, offset, read);
        }

        private unsafe int ReadInternal(byte[] buffer, int offset, int read)
        { 
            var totalToRead = read < BufferWindowSize ? read : BufferWindowSize;

            var pDest = _bufferWindow.Pointer;
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

        public unsafe byte[] PeepInReadStream()
        {
            // return the last 4K starting at the last position in the array,
            // and continue to copy from the start of the array till the last position.
            // however if the buffer wasn't overruning its tail (_firstWindow == true) then
            // we copy from the start to last position only. 
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
            while ((_bufferWindow.Buffer.Array[start] & 0x80) != 0)
            {
                start++;
                size--;
                if (start >= BufferWindowSize)
                    start = 0;
            }
            var buf = new byte[size];
            if (size == 0)
                return buf;
            byte* pSrc = _bufferWindow.Pointer;
            fixed (byte* pDest = buf)
            {
                var firstSize = BufferWindowSize - start;
                Memory.Copy(pDest, pSrc+ start, firstSize);
                Memory.Copy(pDest + firstSize, pSrc, start);
                return buf;
            }
        }

        public async Task<int> ReadAsync(byte[] buffer, int offset, int count)
        {
            var read = await _stream.ReadAsync(buffer, offset, count).ConfigureAwait(false);
            var rc = ReadInternal(buffer, offset, read);

            return rc;
        }

        public void Dispose()
        {
            // we do not dispose _stream
            _returnedBuffer.Dispose();
        }
    }
}
