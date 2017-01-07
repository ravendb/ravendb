using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using Sparrow.Binary;
using Sparrow.Json.Parsing;

namespace Sparrow.Json
{
    public unsafe interface IUnmanagedWriteBuffer : IDisposable
    {
        int SizeInBytes { get; }
        void Write(byte[] buffer, int start, int count);
        void Write(byte* buffer, int length);
        void WriteByte(byte data);
        void EnsureSingleChunk(JsonParserState state);
        void EnsureSingleChunk(out byte* ptr, out int size);
    }

    public unsafe struct UnmanagedStreamBuffer : IUnmanagedWriteBuffer
    {
        private readonly Stream _stream;
        private int _sizeInBytes;
        public int Used;
        private readonly JsonOperationContext.ManagedPinnedBuffer _buffer;
        private JsonOperationContext.ReturnBuffer _returnBuffer;

        public int SizeInBytes => _sizeInBytes;

        public UnmanagedStreamBuffer(JsonOperationContext context, Stream stream)
        {
            _stream = stream;
            _sizeInBytes = 0;
            Used = 0;
            _returnBuffer = context.GetManagedBuffer(out _buffer);
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
                if (Used == _buffer.Length)
                {
                    _stream.Write(_buffer.Buffer.Array, _buffer.Buffer.Offset, Used);
                    Used = 0;
                }

                var bytesToWrite = Math.Min(lengthLeft, _buffer.Length - Used);

                Memory.Copy(_buffer.Pointer + Used, buffer, bytesToWrite);
                _sizeInBytes += bytesToWrite;
                lengthLeft -= bytesToWrite;
                bufferPosition += bytesToWrite;
                buffer += bytesToWrite;
                Used += bytesToWrite;

            } while (bufferPosition < length);
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteByte(byte data)
        {
            if (Used == _buffer.Length)
            {
                _stream.Write(_buffer.Buffer.Array, _buffer.Buffer.Offset, Used);
                Used = 0;
            }
            _sizeInBytes++;
            *(_buffer.Pointer + Used) = data;
            Used++;
        }

        public int CopyTo(IntPtr pointer)
        {
            throw new NotSupportedException();
        }

        public int CopyTo(byte* pointer)
        {
            throw new NotSupportedException();
        }

        public void Dispose()
        {
            _returnBuffer.Dispose();
            if (Used == 0)
                return;

            _stream.Write(_buffer.Buffer.Array, _buffer.Buffer.Offset, Used);
            Used = 0;
        }

        public void EnsureSingleChunk(JsonParserState state)
        {
            throw new NotSupportedException();
        }
        public void EnsureSingleChunk(out byte* ptr, out int size)
        {
            throw new NotSupportedException();
        }
    }

    public unsafe struct UnmanagedWriteBuffer : IUnmanagedWriteBuffer
    {
        private readonly JsonOperationContext _context;
        private int _sizeInBytes;

        private class Segment
        {
            public Segment Previous;
            public Segment PreviousAllocated;
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
                Previous = _current,
                PreviousAllocated = _current
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
                _current = _current.PreviousAllocated;
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
