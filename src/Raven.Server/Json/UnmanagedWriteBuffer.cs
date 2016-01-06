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

        public UnmanagedWriteBuffer(UnmanagedBuffersPool buffersPool, string documentId)
        {
            _buffersPool = buffersPool;
            _documentId = documentId;
            int size;
            _current = new Segment
            {
                Address = _buffersPool.GetMemory(4096, documentId, out size),
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
            var nextSegmentSize = Math.Max(_current.ActualSize*2, (int)Voron.Util.Utils.NearestPowerOfTwo(required));
            _current = new Segment
            {
                Prev = _current,
                Address = _buffersPool.GetMemory(nextSegmentSize, _documentId, out nextSegmentSize),
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
