using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write(byte[] buffer, int start, int count)
        {
            fixed (byte* p = buffer)
            {
                Write(p + start, count);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write(byte* buffer, int count)
        {
            if (count == 0)
                return;

            if (_buffer.Length - Used > count)
            {
                Memory.Copy(_buffer.Pointer + Used, buffer, (uint)count);
                _sizeInBytes += count;
                Used += count;
            }
            else
                UnlikelyWrite(buffer, count);
        }

        private void UnlikelyWrite(byte* buffer, int count)
        {
            if (count == 0)
                return;

            var bufferPosition = 0;
            var lengthLeft = count;
            do
            {
                if (Used == _buffer.Length)
                {
                    _stream.Write(_buffer.Buffer.Array, _buffer.Buffer.Offset, Used);
                    Used = 0;
                }

                var bytesToWrite = Math.Min(lengthLeft, _buffer.Length - Used);

                Memory.Copy(_buffer.Pointer + Used, buffer, (uint)bytesToWrite);

                _sizeInBytes += bytesToWrite;
                lengthLeft -= bytesToWrite;
                bufferPosition += bytesToWrite;
                buffer += bytesToWrite;
                Used += bytesToWrite;

            } while (bufferPosition < count);
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
            using (_returnBuffer)
            {
                if (Used != 0)
                    _stream.Write(_buffer.Buffer.Array, _buffer.Buffer.Offset, Used);
                Used = 0;
            }
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

        private class Segment
        {
            /// <summary>
            /// This points to the previous Segment in the stream. May be null
            /// due either to a Clean operation, or because none have been
            /// allocated
            /// </summary>
            public Segment Previous;

            /// <summary>
            /// Every Segment in this linked list is freed when the 
            /// UnmanagedWriteBuffer is disposed. Kept for resilience against
            /// Clean operations
            /// </summary>
            public Segment DeallocationPendingPrevious;

            /// <summary>
            /// Memory in this Segment
            /// </summary>
            public AllocatedMemoryData Allocation;

            /// <summary>
            /// Always set to Allocation.Adddress
            /// </summary>
            public byte* Address;

            /// <summary>
            /// Used bytes in the current Segment
            /// </summary>
            public int Used;

            /// <summary>
            /// Total size accumulated by all the previous Segments
            /// </summary>
            public int AccumulatedSizeInBytes;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public Segment ShallowCopy()
            {
                return (Segment)MemberwiseClone();
            }

#if DEBUG
            public int Depth
            {
                get
                {
                    int count = 1;
                    var prev = Previous;
                    while (prev != null)
                    {
                        count++;
                        prev = prev.Previous;
                    }
                    return count;
                }
            }

            public string DebugInfo => Encodings.Utf8.GetString(Address, Used);
#endif
        }

        private Segment _head;

        public int SizeInBytes
        {
            get
            {
                ThrowOnDisposed();
                return _head.AccumulatedSizeInBytes;
            }
        }

        // Since we never know which instance actually ran the Dispose, it is 
        // possible that this particular copy may have _head != null.
        public bool IsDisposed => _head == null || _head.Address == null;

        public UnmanagedWriteBuffer(JsonOperationContext context, AllocatedMemoryData allocatedMemoryData)
        {
            Debug.Assert(context != null);
            Debug.Assert(allocatedMemoryData != null);

            _context = context;
            _head = new Segment
            {
                Previous = null,
                DeallocationPendingPrevious = null,
                Allocation = allocatedMemoryData,
                Address = allocatedMemoryData.Address,
                Used = 0,
                AccumulatedSizeInBytes = 0
            };

#if MEM_GUARD
            AllocatedBy = Environment.StackTrace;
            FreedBy = null;
#endif
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write(byte[] buffer, int start, int count)
        {
            Debug.Assert(start >= 0 && start < buffer.Length); // start is an index
            Debug.Assert(count >= 0); // count is a size
            Debug.Assert(start + count <= buffer.Length); // can't overrun the buffer

            fixed (byte* bufferPtr = buffer)
            {
                Debug.Assert(bufferPtr + start >= bufferPtr); // overflow check
                Write(bufferPtr + start, count);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ThrowOnDisposed()
        {
#if DEBUG
            // PERF: This check will only happen in debug mode because it will fail with a NRE anyways on release.
            if (IsDisposed)
                throw new ObjectDisposedException(nameof(UnmanagedWriteBuffer));
#endif
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write(byte* buffer, int count)
        {
            Debug.Assert(count >= 0); // count is a size
            Debug.Assert(buffer + count >= buffer); // overflow check
            ThrowOnDisposed();

            if (count == 0)
                return;

            var head = _head;
            if (head.Allocation.SizeInBytes - head.Used > count)
            {
                Memory.Copy(head.Address + head.Used, buffer, (uint)count);
                head.AccumulatedSizeInBytes += count;
                head.Used += count;
            }
            else
                WriteUnlikely(buffer, count);
        }

        private void WriteUnlikely(byte* buffer, int count)
        {
            Debug.Assert(count >= 0); // count is a size
            Debug.Assert(buffer + count >= buffer); // overflow check

            var amountPending = count;
            var head = _head;
            do
            {
                var availableSpace = head.Allocation.SizeInBytes - head.Used;
                // If the current Segment does not have any space left, allocate a new one
                if (availableSpace == 0)
                {
                    AllocateNextSegment(amountPending, true);
                    head = _head;
                }

                // Write as much as we can in the current Segment
                var amountWrittenInRound = Math.Min(amountPending, availableSpace);
                Memory.Copy(head.Address + head.Used, buffer, (uint)amountWrittenInRound);

                // Update Segment invariants
                head.AccumulatedSizeInBytes += amountWrittenInRound;
                head.Used += amountWrittenInRound;

                // Update loop invariants
                amountPending -= amountWrittenInRound;
                buffer += amountWrittenInRound;
            } while (amountPending > 0);
        }

        private void AllocateNextSegment(int required, bool allowGrowth)
        {
            Debug.Assert(required > 0);

            // Grow by doubling segment size until we get to 1 MB, then just use 1 MB segments
            // otherwise a document with 17 MB will waste 15 MB and require very big allocations
            var segmentSize = Math.Max(Bits.NextPowerOf2(required), _head.Allocation.SizeInBytes * 2);
            const int oneMb = 1024 * 1024;
            if (segmentSize > oneMb && required <= oneMb)
                segmentSize = oneMb;

            // We can sometimes ask the context to grow the allocation size; 
            // it may do so at its own discretion; if this happens, then we
            // are good to go.
            if (allowGrowth && _context.GrowAllocation(_head.Allocation, segmentSize))
                return;

            // Can't change _head because there may be copies of the current
            // instance of UnmanagedWriteBuffer going around. Thus, we simply
            // mutate it to ensure all copies have the same allocations.
            var allocation = _context.GetMemory(segmentSize);

            // Copy the head
            Segment previousHead = _head.ShallowCopy();

            // Reset the head (this change happens in all instances at the
            // same time, albeit not atomically).
            _head.Previous = previousHead;
            _head.DeallocationPendingPrevious = previousHead;
            _head.Allocation = allocation;
            _head.Address = allocation.Address;
            _head.Used = 0;
            _head.AccumulatedSizeInBytes = previousHead.AccumulatedSizeInBytes;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteByte(byte data)
        {
            ThrowOnDisposed();

            var head = _head;
            if (head.Used == head.Allocation.SizeInBytes)
                goto Grow; // PERF: Diminish the size of the most common path.

            head.AccumulatedSizeInBytes++;
            *(head.Address + head.Used) = data;
            head.Used++;
            return;

Grow:
            WriteByteUnlikely(data);
        }

        private void WriteByteUnlikely(byte data)
        {
            AllocateNextSegment(1, true);
            var head = _head;
            head.AccumulatedSizeInBytes++;
            *(head.Address + head.Used) = data;
            head.Used++;
        }

        public int CopyTo(byte* pointer)
        {
            ThrowOnDisposed();

            var whereToWrite = pointer + _head.AccumulatedSizeInBytes;
            var copiedBytes = 0;

            for (var head = _head; head != null; head = head.Previous)
            {
                whereToWrite -= head.Used;
                copiedBytes += head.Used;
                Memory.Copy(whereToWrite, head.Address, head.Used);
            }
            Debug.Assert(copiedBytes == _head.AccumulatedSizeInBytes);
            return copiedBytes;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Clear()
        {
            ThrowOnDisposed();

            _head.Used = 0;
            _head.AccumulatedSizeInBytes = 0;
            _head.Previous = null;
        }

        public void Dispose()
        {
            if (IsDisposed)
                return;

#if MEM_GUARD
            FreedBy = Environment.StackTrace;
#endif

            // The actual lifetime of the _head Segment is unbounded: it will
            // be released by the GC when we no longer have any references to
            // it (i.e. no more copies of this struct)
            //
            // We can, however, force the deallocation of all the previous
            // Segments by ensuring we don't keep any references after the
            // Dispose is run.

            var head = _head;
            _head = null; // Further references are NREs.
            for (Segment next; head != null; head = next)
            {
                _context.ReturnMemory(head.Allocation);

                // This is used to signal that Dispose has run to other copies
                head.Address = null;

#if DEBUG
                // Helps to avoid program errors, albeit unnecessary
                head.Allocation = null;
                head.AccumulatedSizeInBytes = -1;
                head.Used = -1;
#endif

                // `next` is used to keep a reference to the previous Segment.
                // Since `next` lives only within this for loop and we clear up
                // all other references, non-head Segments should be GC'd.
                next = head.DeallocationPendingPrevious;
                head.Previous = null;
                head.DeallocationPendingPrevious = null;
            }
        }

#if MEM_GUARD
        public string AllocatedBy;
        public string FreedBy;
#endif

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void EnsureSingleChunk(JsonParserState state)
        {
            EnsureSingleChunk(out state.StringBuffer, out state.StringSize);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void EnsureSingleChunk(out byte* ptr, out int size)
        {
            ThrowOnDisposed();

            if (_head.Previous == null)
            {
                // Common case is we have a single chunk already, so no need 
                // to do anything
                ptr = _head.Address;
                size = SizeInBytes;
                return;
            }
            
            UnlikelyEnsureSingleChunk(out ptr, out size);
        }

        private void UnlikelyEnsureSingleChunk(out byte* ptr, out int size)
        {
            // we are using multiple segments, but the current one can fit all
            // the required memory
            if (_head.Allocation.SizeInBytes - _head.Used > SizeInBytes)
            {
                CopyTo(_head.Address + _head.Used);
                // we need to fit in the beginning of the chunk, so we must move it backward.
                Memory.Move(_head.Address, _head.Address + _head.Used, SizeInBytes);

                ptr = _head.Address;
                size = SizeInBytes;
                _head.Used = SizeInBytes;
                // Ensure we are thought of as a single chunk
                _head.Previous = null;
                return;
            }

            var totalSize = SizeInBytes;
            
            // We might need to allocate, but we don't want to allocate the usual power of 2 * 3 
            // because we know _exactly_ what we need
            using (_context.AvoidOverAllocation())
            {
                // If we are here, then we have multiple chunks, we can't
                // allow a growth of the last chunk, since we'll by copying over it
                // so we force a whole new chunk
                AllocateNextSegment(totalSize, false);
            }

            // Go back in time to before we had the last chunk
            var realHead = _head;
            _head = realHead.Previous;

            // Copy all of the data structure into the new chunk's memory
            CopyTo(realHead.Address);
            realHead.Used = totalSize;
            realHead.AccumulatedSizeInBytes = totalSize;

            // Back to the future!
            _head = realHead;

            // Ensure we are thought of as a single chunk
            _head.Previous = null;

            ptr = _head.Address;
            size = _head.Used;
        }
    }
}
