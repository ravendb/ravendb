//-----------------------------------------------------------------------
// <copyright file="MemoryCache.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    using System;
    using System.Diagnostics;
    using System.Threading;

    /// <summary>
    /// Cache allocated chunks of memory that are needed for very short periods
    /// of time. The memory is not zeroed on allocation.
    /// </summary>
    internal sealed class MemoryCache
    {
        /// <summary>
        /// A zero-length array that should be used whenever we want to return one.
        /// </summary>
        private static readonly byte[] zeroLengthArray = new byte[0];

        /// <summary>
        /// Default size for newly allocated buffers.
        /// </summary>
        private readonly int bufferSize;
        
        /// <summary>
        /// Currently cached buffers.
        /// </summary>
        private readonly byte[][] cachedBuffers;

        /// <summary>
        /// Initializes a new instance of the <see cref="MemoryCache"/> class. 
        /// </summary>
        /// <param name="bufferSize">
        /// The size of the buffers to cache.
        /// </param>
        /// <param name="maxCachedBuffers">
        /// The maximum number of buffers to cache.
        /// </param>
        public MemoryCache(int bufferSize, int maxCachedBuffers)
        {
            this.bufferSize = bufferSize;
            this.cachedBuffers = new byte[maxCachedBuffers][];
        }

        /// <summary>
        /// Gets the size of the buffers that this cache returns.
        /// </summary>
        public int BufferSize
        {
            get
            {
                return this.bufferSize;
            }
        }

        /// <summary>
        /// Creates a new array containing a copy of 'length' bytes of data.
        /// </summary>
        /// <param name="data">The data to copy.</param>
        /// <param name="length">The length of data to copy.</param>
        /// <returns>An array containing the first length bytes of data.</returns>
        public static byte[] Duplicate(byte[] data, int length)
        {
            Debug.Assert(data.Length >= length, "length parameter is too long");
            if (0 == length)
            {
                return zeroLengthArray;
            }

            var output = new byte[length];
            Buffer.BlockCopy(data, 0, output, 0, length);
            return output;
        }

        /// <summary>
        /// Allocates a chunk of memory. If memory is cached it is returned. If no memory
        /// is cached then it is allocated. Check the size of the returned buffer to determine
        /// how much memory was allocated.
        /// </summary>
        /// <returns>A new memory buffer.</returns>
        public byte[] Allocate()
        {
            int offset = this.GetStartingOffset();
            for (int i = 0; i < this.cachedBuffers.Length; ++i)
            {
                int index = (i + offset) % this.cachedBuffers.Length;
                byte[] buffer = Interlocked.Exchange(ref this.cachedBuffers[index], null);
                if (null != buffer)
                {
                    return buffer;
                }
            }

           return new byte[this.bufferSize];
        }

        /// <summary>
        /// Frees an unused buffer. This may be added to the cache.
        /// </summary>
        /// <param name="data">The memory to free.</param>
        public void Free(ref byte[] data)
        {
            if (null == data)
            {
                throw new ArgumentNullException("data");
            }

            if (data.Length != this.bufferSize)
            {
                throw new ArgumentOutOfRangeException("data", data.Length, "buffer is not correct size for this MemoryCache");    
            }

            int offset = this.GetStartingOffset();

            // The buffers are garbage collected so we don't need to make Free()
            // completely safe. In a multi-threaded situation we may see a null
            // slot and then overwrite a buffer which was just freed into the slot.
            // That will cause us to lose a buffer which could have been placed
            // in a different slot, but in return we can do the Free() without
            // expensive interlocked operations.
            for (int i = 0; i < this.cachedBuffers.Length; ++i)
            {
                int index = (i + offset) % this.cachedBuffers.Length;
                if (null == this.cachedBuffers[index])
                {
                    this.cachedBuffers[index] = data;
                    break;
                }
            }

            data = null;
        }

        /// <summary>
        /// Get the offset in the cached buffers array to start allocating or freeing 
        /// buffers to. This is done so that all threads don't start operating on
        /// slot zero, which would increase contention.
        /// </summary>
        /// <returns>The starting offset for Allocate/Free operations.</returns>
        private int GetStartingOffset()
        {
            // Using the current CPU number would be ideal, but there doesn't seem to 
            // be a cheap way to get that information in managed code.
            return Thread.CurrentThread.ManagedThreadId % this.cachedBuffers.Length;
        }
    }
}