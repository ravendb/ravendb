using System;
using System.Buffers;
using System.Runtime.CompilerServices;
using Sparrow.Json;

namespace Raven.Client.Json.Serialization.SystemTextJson.Internal
{
    /// <summary>
    /// An IBufferWriter backed by JsonOperationContext arena memory.
    /// Avoids managed byte[] allocations entirely - all memory comes from the context's
    /// native arena allocator and is freed when the context is reset/disposed.
    /// </summary>
    internal sealed unsafe class ContextMemoryBufferWriter : IBufferWriter<byte>, IDisposable
    {
        private readonly JsonOperationContext _context;
        private AllocatedMemoryData _allocation;
        private int _position;

        public ContextMemoryBufferWriter(JsonOperationContext context, int initialSize = 4096)
        {
            _context = context;
            _allocation = context.GetMemory(initialSize);
            _position = 0;
        }

        public ReadOnlySpan<byte> WrittenSpan => new ReadOnlySpan<byte>(_allocation.Address, _position);

        public int WrittenCount => _position;

        public void Advance(int count)
        {
            _position += count;
        }

        public Memory<byte> GetMemory(int sizeHint = 0)
        {
            EnsureCapacity(sizeHint);
            return _allocation.AsMemory().Slice(_position);
        }

        public Span<byte> GetSpan(int sizeHint = 0)
        {
            EnsureCapacity(sizeHint);
            return new Span<byte>(_allocation.Address + _position, _allocation.SizeInBytes - _position);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void EnsureCapacity(int sizeHint)
        {
            if (sizeHint <= 0)
                sizeHint = 1;

            int remaining = _allocation.SizeInBytes - _position;
            if (remaining >= sizeHint)
                return;

            Grow(sizeHint);
        }

        private void Grow(int sizeHint)
        {
            int needed = _position + sizeHint;
            int increase = needed - _allocation.SizeInBytes;

            // Try to grow in-place first (just bumps the arena pointer, no copy)
            if (_context.GrowAllocation(_allocation, increase))
                return;

            // Can't grow in-place - allocate new, copy, return old
            int newSize = Math.Max(_allocation.SizeInBytes * 2, needed);
            var newAllocation = _context.GetMemory(newSize);
            Buffer.MemoryCopy(_allocation.Address, newAllocation.Address, newAllocation.SizeInBytes, _position);
            _context.ReturnMemory(_allocation);
            _allocation = newAllocation;
        }

        /// <summary>
        /// Return native memory to the context immediately.
        /// Call after the written bytes have been consumed.
        /// </summary>
        public void ReturnMemory()
        {
            if (_allocation != null)
            {
                _context.ReturnMemory(_allocation);
                _allocation = null;
            }
        }

        public void Dispose()
        {
            ReturnMemory();
        }
    }
}
