using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Json;

namespace Sparrow
{
    public class PeepingTomStream : IDisposable
    {
        public const int BufferWindowSize = 4096;

        private readonly JsonOperationContext.ManagedPinnedBuffer _bufferWindow;
        private int _pos;
        private readonly Stream _stream;
        private bool _firstWindow = true;
        private JsonOperationContext.ReturnBuffer _returnedBuffer;

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

                _pos += totalToRead;

                if (_pos > BufferWindowSize) // copy in two parts
                {
                    var newTotal = BufferWindowSize - (_pos - totalToRead);
                    Memory.Copy(pBufferWindowStart, pBufferStart, newTotal);
                    var nextLength = totalToRead - newTotal;
                    Debug.Assert(nextLength <= BufferWindowSize);
                    Memory.Copy(pDest, pBufferStart + newTotal, nextLength);

                    _firstWindow = false;
                    _pos %= BufferWindowSize;
                }
                else
                {
                    Memory.Copy(pBufferWindowStart, pBufferStart, totalToRead);
                }
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
            // search for the first byte which represent a single UTF charcter
            // (because 'start' might point to a byte in a middle of set of bytes
            // representing single character, so 0x80 represent start of char in utf8)
            var originalStart = start;

            for (var p = _bufferWindow.Pointer; (*(p + start) & 0x80) != 0; p++)
            {
                start++;                
                size--;
                
                // requested size doesn't contains utf8 character
                if (size == 0)
                    return new byte[0];
                
                // looped through the entire buffer without utf8 character found
                if (start == originalStart)
                    return new byte[0];

                if (start >= BufferWindowSize)
                    start = 0;
            }
            var buf = new byte[size];
            if (size == 0)
                return buf;
            byte* pSrc = _bufferWindow.Pointer;
            fixed (byte* pDest = buf)
            {
                var firstSize = size - start;
                Memory.Copy(pDest, pSrc+ start, firstSize);
                Memory.Copy(pDest + firstSize, pSrc, start);
                return buf;
            }
        }

        public async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken token = default)
        {
            var read = await _stream.ReadAsync(buffer, offset, count, token).ConfigureAwait(false);
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
