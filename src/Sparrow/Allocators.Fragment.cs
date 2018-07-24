using System;
using System.Collections.Generic;
using System.Reflection;
using Sparrow.Global;

namespace Sparrow
{
    public interface IFragmentAllocatorOptions : IAllocatorOptions
    {
        int ReuseBlocksBiggerThan { get; }
        int AllocationBlockSizeInBytes { get; }

        IAllocatorComposer<Pointer> CreateAllocator();
    }

    public static class FragmentAllocator
    {
        public struct Default : IFragmentAllocatorOptions, INativeOptions
        {
            public bool UseSecureMemory => false;
            public bool ElectricFenceEnabled => false;
            public bool Zeroed => false;

            public int ReuseBlocksBiggerThan => 128 * Constants.Size.Kilobyte;
            public int AllocationBlockSizeInBytes => 4 * Constants.Size.Megabyte;

            public IAllocatorComposer<Pointer> CreateAllocator()
            {
                var allocator = new Allocator<NativeAllocator<Default>>();
                allocator.Initialize(default(Default));
                return allocator;
            }
        }
    }

    /// <summary>
    /// The FragmentAllocator will hold all the memory it can during the process, he will even reuse fragments when big enough without releasing them to the underlying allocator.
    /// </summary>
    /// <typeparam name="TOptions">The options to use for the allocator.</typeparam>
    /// <remarks>The Options object must be properly implemented to achieve performance improvements. (use constants as much as you can on configuration)</remarks>
    public unsafe struct FragmentAllocator<TOptions> : IAllocator<FragmentAllocator<TOptions>, Pointer>, IAllocator
        where TOptions : struct, IFragmentAllocatorOptions
    {
        private TOptions _options;
        private IAllocatorComposer<Pointer> _internalAllocator;

        private Pointer _currentBuffer;

        // The start of the current arena chunk
        private byte* _ptrStart;
        // The current position over the current arena chunk
        private byte* _ptrCurrent;
        
        // How many bytes has this arena handed out to the customers.
        private long _used;

        private SortedList<int, Pointer> _internalReadyToUseMemorySegments;
    
        // Buffers that has been used and cannot be cleaned until a reset happens.
        private List<Pointer> _allocatedSegments;

        // How many bytes has this arena allocated from its memory provider since resets
        public long Allocated { get; private set; }

        public void Initialize(ref FragmentAllocator<TOptions> allocator)
        {
            allocator._internalReadyToUseMemorySegments = new SortedList<int, Pointer>(new DuplicateKeyComparer<int>());
            allocator._allocatedSegments = new List<Pointer>();
        }

        public void Configure<TConfig>(ref FragmentAllocator<TOptions> allocator, ref TConfig configuration) where TConfig : struct, IAllocatorOptions
        {
            if (!typeof(TOptions).GetTypeInfo().IsAssignableFrom(typeof(TConfig)))
                throw new NotSupportedException($"{nameof(TConfig)} is not compatible with {nameof(TOptions)}");

            // This cast will get evicted by the JIT. 
            allocator._options = (TOptions)(object)configuration;
            allocator._internalAllocator = allocator._options.CreateAllocator();
        }

        public Pointer Allocate(ref FragmentAllocator<TOptions> allocator, int size)
        {
            if (size < allocator._options.AllocationBlockSizeInBytes)
            {                
                if (allocator._used + size > allocator._currentBuffer.Size || _ptrStart == null)
                {
                    // PERF: This should be considered cold code.
                    // TODO: Check the code layout.
                    AllocateSegment(ref allocator, size);
                }

                Pointer ptr = new Pointer(allocator._ptrCurrent, size);
                allocator._ptrCurrent += size;
                allocator._used += size;
                allocator.Allocated += size;

                return ptr;
            }

            // The allocation is too big to fit, the memory will be used afterwards as a segment when released.
            // PERF: For this kind of allocations we dont care if we hit cold code.
            Pointer segment = allocator._internalAllocator.Allocate(size);
            allocator._allocatedSegments.Add(segment);
            return segment;
        }

        /// <summary>
        /// Comparer for comparing two keys, handling equality as beeing greater
        /// Use this Comparer e.g. with SortedLists or SortedDictionaries, that don't allow duplicate keys
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        public class DuplicateKeyComparer<TKey> : IComparer<TKey> where TKey : IComparable
        {
            public int Compare(TKey x, TKey y)
            {
                int result = x.CompareTo(y);

                if (result == 0)
                    return 1;   // Handle equality as beeing greater
               return result;
            }
        }

        private void AllocateSegment(ref FragmentAllocator<TOptions> allocator, int size)
        {
            int bytesLeft = (int)(allocator._currentBuffer.Size - allocator._used);
            if (bytesLeft > allocator._options.ReuseBlocksBiggerThan)
            {
                // The allocation is bigger than ReuseBlocksBiggerThan and therefore we can leave this one available to be used later.
                allocator._internalReadyToUseMemorySegments.Add(bytesLeft, new Pointer(allocator._ptrCurrent, bytesLeft));
            }

            // TODO: Time the impact of using a heap structure instead of a SortedList.
            // Get the biggest segment on the Heap and if size < the biggest one, we avoid the allocation.
            Pointer segment;
            if (allocator._allocatedSegments.Count > 0 && allocator._allocatedSegments[0].Size >= size)
            {
                segment = allocator._allocatedSegments[0];
                allocator._allocatedSegments.RemoveAt(0);
            }
            else
            {
                segment = allocator._internalAllocator.Allocate(allocator._options.AllocationBlockSizeInBytes);
                allocator._allocatedSegments.Add(segment);
            }

            allocator._currentBuffer = segment;
            allocator._ptrStart = (byte*)segment.Ptr;
            allocator._ptrCurrent = (byte*)segment.Ptr;

            allocator._used = 0;            
        }

        public void Release(ref FragmentAllocator<TOptions> allocator, ref Pointer ptr)
        {
            byte* address = (byte*)ptr.Ptr;
            if (address < allocator._ptrStart || address != allocator._ptrCurrent - ptr.Size)
            {
                // We have fragmentation. note that this fragmentation will be healed by the call to Reset
                // trying to do this on the fly is too expensive unless the chunk is big enough to consider it a whole segment for himself.
                // Consider to compose this allocator with a PoolAllocator instead if high fragmentation is expected. 
                if (ptr.Size > allocator._options.ReuseBlocksBiggerThan)
                    allocator._internalReadyToUseMemorySegments.Add(ptr.Size, ptr);

                return;
            }

            // since the returned allocation is at the end of the arena, we can just move
            // the pointer back
            allocator._used -= ptr.Size;
            allocator.Allocated -= ptr.Size;
            allocator._ptrCurrent -= ptr.Size;
        }

        public void Reset(ref FragmentAllocator<TOptions> allocator)
        {
            // We are not going to track this memory chunks anymore.
            allocator._internalReadyToUseMemorySegments.Clear();

            foreach (var segment in allocator._allocatedSegments)
            {
                var ptr = segment;
                allocator._internalAllocator.Release(ref ptr);
            }
        }

        public void OnAllocate(ref FragmentAllocator<TOptions> allocator, Pointer ptr) {}
        public void OnRelease(ref FragmentAllocator<TOptions> allocator, Pointer ptr) {}

        public void Dispose(ref FragmentAllocator<TOptions> allocator)
        {
            // We are not going to track this memory chunks anymore.
            allocator._internalReadyToUseMemorySegments.Clear();

            foreach (var segment in allocator._allocatedSegments)
            {
                var ptr = segment;
                allocator._internalAllocator.Release(ref ptr);
            }

            allocator._allocatedSegments.Clear();
            allocator._ptrStart = null;
            allocator._ptrCurrent = null;
            allocator._used = 0;
        }
    }
}
