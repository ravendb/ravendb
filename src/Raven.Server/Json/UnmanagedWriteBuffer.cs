using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;
using Sparrow;
using Sparrow.Binary;

namespace Raven.Server.Json
{
    public unsafe class UnmanagedWriteBuffer : IDisposable
    {
        private readonly RavenOperationContext _context;

        private class Segment
        {
            public Segment Previous;
            public UnmanagedBuffersPool.AllocatedMemoryData Allocation;
            public byte* Address;
            public int Used;

            public string DebugInfo => Encoding.UTF8.GetString(Address, Used);
        }

        private Segment _current;

        private int _sizeInBytes;
        private bool _disposed;

        public int SizeInBytes => _sizeInBytes;

        public UnmanagedWriteBuffer(RavenOperationContext context)
        {
            _context = context;
            var allocatedMemoryData = _context.GetMemory(4096);
            _current = new Segment
            {
                Address = (byte*)allocatedMemoryData.Address,
                Allocation = allocatedMemoryData,
                Used = 0,
                Previous = null
            };
        }

        public void Write(byte* buffer, int length)
        {
            if (length == 0)
                return;

            var bufferPosition = 0;
            var lengthLeft = length;
            do
            {
                // Create next, bigger segment if needed
                if (_current.Allocation.SizeInBytes == _current.Used)
                {
                    AllocateNextSegment(lengthLeft);
                }

                var bytesToWrite = Math.Min(lengthLeft, _current.Allocation.SizeInBytes - _current.Used);

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
                Math.Max(_current.Allocation.SizeInBytes * 2, (int)Bits.NextPowerOf2(required))
                );
            var allocatedMemoryData = _context.GetMemory(nextSegmentSize);
            _current = new Segment
            {
                Address = (byte*)allocatedMemoryData.Address,
                Allocation = allocatedMemoryData,
                Used = 0,
                Previous = _current
            };
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteByte(byte data)
        {
            if (_current.Used == _current.Allocation.SizeInBytes)
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
                cur = cur.Previous;
            }
            Debug.Assert(copiedBytes == _sizeInBytes);
            return copiedBytes;
        }

        public void Clear(int sizeRequired = 0)
        {
            _current.Used = 0;
            _sizeInBytes = 0;

            // this releases everything but the current item
            var prev = _current.Previous;
            _current.Previous = null;
            while (prev != null)
            {
                _context.ReturnMemory(prev.Allocation);
                prev = prev.Previous;
            }

            if (_current.Allocation.SizeInBytes < sizeRequired)
            {
                _context.ReturnMemory(_current.Allocation);
                _current = null;
                AllocateNextSegment(sizeRequired);
            }
        }

        public void Dispose()
        {
            if (_disposed)
                return;

            var cur = _current;
            while (cur != null)
            {
                _context.ReturnMemory(cur.Allocation);
                cur = cur.Previous;
            }
            _disposed = true;
        }

        public byte* GetBufferFor(int requiredSize)
        {
            var used = _current.Used;
            if (used + requiredSize > _current.Allocation.SizeInBytes)
            {
                throw new ArgumentOutOfRangeException(nameof(requiredSize),
                    "Cannot request a buffer of this size from the buffer, you didn't ensure that it is available first");
            }
            _current.Used += requiredSize;
            _sizeInBytes += requiredSize;
            return _current.Address + used;
        }
    }
}
