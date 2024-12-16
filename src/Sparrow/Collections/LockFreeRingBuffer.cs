using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Sparrow.Binary;

namespace Sparrow.Collections
{
    public sealed class LockFreeRingBuffer<T>
    {
        // Explicit layout to control the exact memory layout
        [StructLayout(LayoutKind.Sequential, Size = 128)]
        internal unsafe struct Cell
        {
            // Sequence number at offset 0
            internal long _sequence;

            private fixed byte _sequencePadding[64 - sizeof(long)];

            // Value at offset 64 to ensure it's on a different cache line
            internal T _value;

            public long Sequence
            {
                get => Volatile.Read(ref _sequence);
                set => Volatile.Write(ref _sequence, value);
            }

            public T Value
            {
                get => _value;
                set => _value = value;
            }
        }

        private readonly int _bufferMask;
        private readonly Cell[] _buffer;

        private long _enqueuePos;
        private long _dequeuePos;

        public LockFreeRingBuffer(int capacity = 64)
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
                _buffer[i].Sequence = i;
        }


        /// <summary>
        /// Attempts to enqueue an item into the ring buffer.
        /// Returns true if successful, false if the buffer is full.
        /// </summary>
        public bool TryEnqueue(in T item)
        {
            long pos = Volatile.Read(ref _enqueuePos);
            ref Cell cell = ref _buffer[pos & _bufferMask];

            while (true)
            {
                // This will perform a Volatile.Read on the sequence number.
                long dif = cell.Sequence - pos;

                if (dif == 0)
                {
                    if (Interlocked.CompareExchange(ref _enqueuePos, pos + 1, pos) == pos)
                        break;
                }
                else if (dif < 0)
                {
                    // Buffer is full
                    return false;
                }

                pos = Volatile.Read(ref _enqueuePos);
                cell = ref _buffer[pos & _bufferMask];
            }

            cell.Value = item;
            cell.Sequence = pos + 1; // This will perform a Volatile.Write on the sequence number.
            return true;
        }

        /// <summary>
        /// Attempts to dequeue an item from the ring buffer.
        /// Returns true if successful, false if the buffer is empty.
        /// </summary>
        public bool TryDequeue(out T item)
        {
            long pos = Volatile.Read(ref _dequeuePos);
            ref Cell cell = ref _buffer[pos & _bufferMask];

            while (true)
            {
                // This will perform a Volatile.Read on the sequence number.
                long dif = cell.Sequence - (pos + 1);

                if (dif == 0)
                {
                    if (Interlocked.CompareExchange(ref _dequeuePos, pos + 1, pos) == pos)
                        break;
                }
                else if (dif < 0)
                {
                    // Buffer is empty
                    Unsafe.SkipInit(out item);
                    return false;
                }

                pos = Volatile.Read(ref _dequeuePos);
                cell = ref _buffer[pos & _bufferMask];
            }

            item = cell.Value;
            cell.Sequence = pos + _buffer.Length; // This will perform a Volatile.Write on the sequence number.
            return true;
        }

        /// <summary>
        /// Checks if the ring buffer is empty.
        /// </summary>
        public int Count
        {
            get
            {
                long currentEnqueuePos = Volatile.Read(ref _enqueuePos);
                Cell enqueueCell = _buffer[currentEnqueuePos & _bufferMask];

                long currentDequeuePos = Volatile.Read(ref _dequeuePos);
                Cell dequeueCell = _buffer[currentDequeuePos & _bufferMask];

                return (int) (enqueueCell.Sequence - dequeueCell.Sequence);
            }
        }

        /// <summary>
        /// Checks if the ring buffer is empty.
        /// </summary>
        public bool IsEmpty
        {
            get
            {
                long currentDequeuePos = Volatile.Read(ref _dequeuePos);
                Cell cell = _buffer[currentDequeuePos & _bufferMask];
                return (cell.Sequence - (currentDequeuePos + 1)) < 0;
            }
        }

        /// <summary>
        /// Checks if the ring buffer is full.
        /// </summary>
        public bool IsFull
        {
            get
            {
                long currentEnqueuePos = Volatile.Read(ref _enqueuePos);
                Cell cell = _buffer[currentEnqueuePos & _bufferMask];
                return (cell.Sequence - currentEnqueuePos) < 0;
            }
        }
    }
}
