using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Sparrow;

namespace Raven.Server.Json
{
    public unsafe class UnmanagedWriteBuffer : IDisposable
    {
        private readonly UnmanagedBuffersPool _buffersPool;
        private readonly string _documentId;

        private class Segment
        {
            public Segment Prev;
            public byte* Address;
            public int ActualSize;
            public int Used;
        }

        private Segment _current;

        private int _sizeInBytes;
        private bool _disposed;

        public int SizeInBytes => _sizeInBytes;

        public UnmanagedWriteBuffer(UnmanagedBuffersPool buffersPool)
        {
            _buffersPool = buffersPool;
            int size;
            _current = new Segment
            {
                Address = _buffersPool.GetMemory(4096, out size),
                ActualSize = size,
            };
        }

        public void Write(byte* buffer, int length)
        {
            var bufferPosition = 0;
            var lengthLeft = length;
            do
            {
                // Create next, bigger segment if needed
                if (_current.ActualSize == _current.Used)
                {
                    AllocateNextSegment(lengthLeft);
                }

                var bytesToWrite = Math.Min(lengthLeft, _current.ActualSize - _current.Used);

                Memory.Copy(_current.Address + _current.Used, buffer, bytesToWrite);
                _sizeInBytes += bytesToWrite;
                lengthLeft -= bytesToWrite;
                bufferPosition += bytesToWrite;
                buffer += bytesToWrite;
                _current.Used += bytesToWrite;

            } while (bufferPosition < length);
        }

        private void AllocateNextSegment(int required)
        {
            // grow by doubling segment size until we get to 1 MB, then just use 1 MB segments
            // otherwise a document with 17 MB will waste 15 MB and require very big allocations
            var nextSegmentSize = Math.Min(
                1024 * 1024 * 1024,
                Math.Max(_current.ActualSize * 2, (int)Voron.Util.Utils.NearestPowerOfTwo(required))
                );
            _current = new Segment
            {
                Prev = _current,
                Address = _buffersPool.GetMemory(nextSegmentSize, out nextSegmentSize),
                ActualSize = nextSegmentSize,
            };
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteByte(byte data)
        {
            if (_current.Used == _current.ActualSize)
            {
                AllocateNextSegment(1);
            }
            _sizeInBytes++;
            *(_current.Address+ _current.Used) = data;
            _current.Used++;
        }

        public int CopyTo(byte* pointer)
        {
            var whereToWrite = pointer + _sizeInBytes;
            var cur = _current;
            var copiedBytes = 0;
            while (cur != null)
            {
                whereToWrite -= cur.Used;
                copiedBytes += cur.Used;
                Memory.Copy(whereToWrite, cur.Address, cur.Used);
                cur = cur.Prev;
            }
            Debug.Assert(copiedBytes == _sizeInBytes);
            return copiedBytes;
        }

        public void Clear()
        {
            // this releases everything but the current item
            var prev = _current.Prev;
            while (prev != null)
            {
                _buffersPool.ReturnMemory(prev.Address);
                prev = _current.Prev;
            }
            _current.Used = 0;
            _sizeInBytes = 0;
        }

        public void Dispose()
        {
            if (_disposed)
                return;

            var cur = _current;
            while (cur != null)
            {
                _buffersPool.ReturnMemory(cur.Address);
                cur = cur.Prev;
            }
            _disposed = true;
        }
    }
}
