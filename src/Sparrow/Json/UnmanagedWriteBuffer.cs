﻿using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;
using Sparrow.Binary;
using Sparrow.Json.Parsing;

namespace Sparrow.Json
{
    public unsafe struct UnmanagedWriteBuffer : IDisposable
    {
        private readonly JsonOperationContext _context;
        private int _sizeInBytes;

        private class Segment
        {
            public Segment Previous;
            public bool PreviousHoldsNoData;
            public AllocatedMemoryData Allocation;
            public byte* Address;
            public int Used;

            public int Depth
            {
                get
                {
                    int count = 1;
                    var prev = Previous;
                    while (prev != null)
                    {
                        if (prev.PreviousHoldsNoData)
                            break;
                        count++;
                        prev = prev.Previous;
                    }
                    return count;
                }
            }
            public string DebugInfo => Encoding.UTF8.GetString(Address, Used);
        }

        private Segment _current;

        public int SizeInBytes => _sizeInBytes;

        internal UnmanagedWriteBuffer(JsonOperationContext context, AllocatedMemoryData allocatedMemoryData)
        {
            _context = context;
            _sizeInBytes=0;
            _current = new Segment
            {
                Address = allocatedMemoryData.Address,
                Allocation = allocatedMemoryData,
                Used = 0,
                Previous = null
            };
        }

        public void Write(byte[] buffer, int start, int count)
        {
            fixed (byte* p = buffer)
            {
                Write(p + start, count);
            }
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
                    AllocateNextSegment(lengthLeft, allowGrowth:true);
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

        private void AllocateNextSegment(int required, bool allowGrowth)
        {
            // grow by doubling segment size until we get to 1 MB, then just use 1 MB segments
            // otherwise a document with 17 MB will waste 15 MB and require very big allocations
            var nextSegmentSize = Math.Max(Bits.NextPowerOf2(required), _current.Allocation.SizeInBytes * 2);
            const int oneMb = 1024*1024;
            if (nextSegmentSize > oneMb && required <= oneMb)
            {
                nextSegmentSize = oneMb;
            }

            if (allowGrowth && 
                // we successfully grew the allocation, nothing to do
                _context.GrowAllocation(_current.Allocation, nextSegmentSize))
                return;

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
                AllocateNextSegment(1, allowGrowth: true);
            }
            _sizeInBytes++;
            *(_current.Address+ _current.Used) = data;
            _current.Used++;
        }

        public int CopyTo(IntPtr pointer)
        {
            return CopyTo((byte*) pointer);
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
                if (cur.PreviousHoldsNoData)
                    break;
                cur = cur.Previous;
            }
            Debug.Assert(copiedBytes == _sizeInBytes);
            return copiedBytes;
        }

        public void Clear()
        {
            _current.Used = 0;
            _sizeInBytes = 0;

            _current.Previous = null;
        }

        public void Dispose()
        {
            while (_current != null)
            {
                _context.ReturnMemory(_current.Allocation);
                _current = _current.Previous;
            }
        }

        public void EnsureSingleChunk(JsonParserState state)
        {
            EnsureSingleChunk(out state.StringBuffer, out state.StringSize);
        }
        public void EnsureSingleChunk(out byte* ptr, out int size)
        {
            if (_current.Previous == null || _current.PreviousHoldsNoData)
            {
                ptr = _current.Address;
                size = _current.Used;
                return;
            }
            // if we are here, then we have multiple chunks, we can't
            // allow a growth of the last chunk, since we'll by copying over it
            // so we force a whole new chunk
            AllocateNextSegment(_sizeInBytes, allowGrowth: false);

            var realCurrent = _current;
            _current = realCurrent.Previous;
            CopyTo(realCurrent.Address);
            realCurrent.Used = SizeInBytes;
            
            _current = realCurrent;
            _current.PreviousHoldsNoData = true;
            ptr = _current.Address;
            size = _current.Used;
        }
    }
}
