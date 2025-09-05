using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Sparrow.Binary;

namespace Sparrow.Collections
{
    public sealed class LockFreeRingBuffer<T>
    {
        private const int CacheLine = 64;

        // Explicit layout to control the exact memory layout
        [StructLayout(LayoutKind.Sequential, Size = 2 * CacheLine)]
        internal unsafe struct Cell
        {
            // Sequence number at offset 0
            internal long Sequence;

            private fixed byte _sequencePadding[64 - sizeof(long)];

            // Value at offset 64 to ensure it's on a different cache line
            internal T Value;
        }

        private readonly int _bufferMask;
        private readonly Cell[] _buffer;

        private PaddedPositions _positions;

        static LockFreeRingBuffer()
        {
            // Fast + generic + big‐value‑friendly – pick any two; with a lock‑free ring buffer
            // the missing one has to be paid for explicitly. We chose fast and generic.
            if (Unsafe.SizeOf<T>() > CacheLine)
                throw new NotSupportedException(
                    $"Type {typeof(T)} is {Unsafe.SizeOf<T>()} B. " +
                    "The ring‑buffer layout guarantees wait‑free behaviour only when the payload " +
                    $"fits in the second cache line (≤ {CacheLine} B). " +
                    "Use a reference or split metadata/payload for larger structs.");
        }

        public LockFreeRingBuffer(int capacity = CacheLine)
        {
            if (capacity < 8)
                throw new ArgumentException("Capacity must be at least 8", nameof(capacity));

            // Ensure capacity is a power of two
            if ((capacity & (capacity - 1)) != 0)
                capacity = Bits.PowerOf2(capacity);
            
            _bufferMask = capacity - 1;

            // Initialize the cells and the sequence numbers.
            _buffer = new Cell[capacity];
            for (long i = 0; i < capacity; i++)
                Volatile.Write(ref _buffer[i].Sequence, i);
        }


        /// <summary>
        /// Attempts to enqueue an item into the ring buffer.
        /// Returns true if successful, false if the buffer is full.
        /// </summary>
        public bool TryEnqueue(in T item)
        {
            long pos = Volatile.Read(ref _positions.Enqueue);
            ref Cell cell = ref _buffer[pos & _bufferMask];

            while (true)
            {
                // This will perform a Volatile.Read on the sequence number.
                long dif = Volatile.Read(ref cell.Sequence) - pos;

                if (dif == 0)
                {
                    if (Interlocked.CompareExchange(ref _positions.Enqueue, pos + 1, pos) == pos)
                        break;
                }
                else if (dif < 0)
                {
                    // Buffer is full
                    return false;
                }

                pos = Volatile.Read(ref _positions.Enqueue);
                cell = ref _buffer[pos & _bufferMask];
            }

            cell.Value = item;
            Volatile.Write(ref cell.Sequence, pos + 1); // This will perform a Volatile.Write on the sequence number.
            return true;
        }

        /// <summary>
        /// Attempts to dequeue an item from the ring buffer.
        /// Returns true if successful, false if the buffer is empty.
        /// </summary>
        public bool TryDequeue(out T item)
        {
            long pos = Volatile.Read(ref _positions.Dequeue);
            ref Cell cell = ref _buffer[pos & _bufferMask];

            while (true)
            {
                // This will perform a Volatile.Read on the sequence number.
                long dif = Volatile.Read(ref cell.Sequence) - (pos + 1);

                if (dif == 0)
                {
                    if (Interlocked.CompareExchange(ref _positions.Dequeue, pos + 1, pos) == pos)
                        break;
                }
                else if (dif < 0)
                {
                    // Buffer is empty
                    Unsafe.SkipInit(out item);
                    return false;
                }

                pos = Volatile.Read(ref _positions.Dequeue);
                cell = ref _buffer[pos & _bufferMask];
            }

            item = cell.Value;
            Volatile.Write(ref cell.Sequence, pos + _buffer.Length); // This will perform a Volatile.Write on the sequence number.
            return true;
        }

        /// <summary>
        /// Checks if the ring buffer is empty.
        /// </summary>
        public int Count
        {
            get
            {
                long currentEnqueuePos = Volatile.Read(ref _positions.Enqueue);
                Cell enqueueCell = _buffer[currentEnqueuePos & _bufferMask];

                long currentDequeuePos = Volatile.Read(ref _positions.Dequeue);
                Cell dequeueCell = _buffer[currentDequeuePos & _bufferMask];

                return (int) (Volatile.Read(ref enqueueCell.Sequence) - Volatile.Read(ref dequeueCell.Sequence));
            }
        }

        /// <summary>
        /// Checks if the ring buffer is empty.
        /// </summary>
        public bool IsEmpty
        {
            get
            {
                long currentDequeuePos = Volatile.Read(ref _positions.Dequeue);
                Cell cell = _buffer[currentDequeuePos & _bufferMask];
                return (Volatile.Read(ref cell.Sequence) - (currentDequeuePos + 1)) < 0;
            }
        }

        /// <summary>
        /// Checks if the ring buffer is full.
        /// </summary>
        public bool IsFull
        {
            get
            {
                long currentEnqueuePos = Volatile.Read(ref _positions.Enqueue);
                Cell cell = _buffer[currentEnqueuePos & _bufferMask];
                return (Volatile.Read(ref cell.Sequence) - currentEnqueuePos) < 0;
            }
        }
    }
    
    /// <summary>
    /// Keep queue and dequeue positions padded along the cache lines.
    /// When padded, the enqueuer does not trash the dequeuer and vice versa.
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = 3 * CacheLine)]
    internal struct PaddedPositions
    {
        private const int CacheLine = 64;
        
        [FieldOffset(CacheLine)]
        public long Enqueue;
        
        [FieldOffset(CacheLine * 2)]
        public long Dequeue;
    }
}
