using System;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Threading;
using Sparrow.Binary;

namespace Sparrow.Collections
{
    // The RingItem makes sure that it doesn't matter how big the struct for the T, it is always guaranteed that the data
    // will be always on an L0 cache line different from the item that comes before. 
    [StructLayout(LayoutKind.Sequential)]
    internal struct RingItem<T>
    {
        private readonly long _p1, _p2, _p3, _p4;

        internal bool IsReady;
        public T Item;

        private readonly long _p5, _p6, _p7, _p8;
    }

    internal sealed class SingleConsumerRingBuffer<T>
    {
        private readonly RingItem<T>[] _buffer;

        private readonly int _size;
        private readonly uint _mask;
        private readonly int _shift;

        private int _startIdx;
        private int _acquiredIdx;
        private int _currentIdx;

        public SingleConsumerRingBuffer(int size)
        {
            // PERF: 'We 'limit' size to be power of 2 in order to make all operations much more efficient without using idiv operations.
            if (!Bits.IsPowerOfTwo(size) || size < 2)
                size = Bits.NextPowerOf2(size);

            _size = size;
            _mask = (uint)size - 1;
            _shift = Bits.MostSignificantBit(size);
            _buffer = new RingItem<T>[size];

            _startIdx = 0;
            _acquiredIdx = -1;
            _currentIdx = 0;
        }

        public bool TryPush(ref T item)
        {
            // We will check if we potentially have the ability to push 1 more item. 
            int cidx = Volatile.Read(ref _currentIdx);
            int sidx = Volatile.Read(ref _startIdx);

            // Check if we have empty spaces in the buffer.
            if (cidx - sidx >= _size)
                return false; // No space to do anything

            int ticker = Interlocked.Increment(ref _currentIdx) - 1;

            // Assign the value
            ref var cl = ref _buffer[ticker & _mask];

            // We assign the item
            cl.Item = item;

            // We then write the volatile value.
            Volatile.Write(ref cl.IsReady, true);
            return true;

            // Very rarely we will go through and try again, the only condition is when we have a huge contention and traffic.
        }

        public bool TryAcquireSingle(out RingItem<T> item)
        {
            int cidx = Volatile.Read(ref _currentIdx);
            int sidx = Volatile.Read(ref _startIdx);

            // We have a pending acquire that hasn't been released. 
            if (_acquiredIdx != -1)
                sidx = Math.Max(_acquiredIdx, sidx);

            int length = cidx - sidx;
            if (length < 1)
            {
                item = default(RingItem<T>);
                return false;
            }

            item = _buffer[sidx & _mask];

            // No need to use volatile here because this is Single Consumer Ring Buffer. 
            _acquiredIdx = sidx + 1;
            return true;
        }

        public Span<RingItem<T>> Acquire()
        {
            int cidx = Volatile.Read(ref _currentIdx);
            int sidx = Volatile.Read(ref _startIdx);

            // We have a pending acquire that hasn't been released. 
            if (_acquiredIdx != -1)
                sidx = Math.Max(_acquiredIdx, sidx);

            int length = cidx - sidx;
            if (length != 0)
            {
                int sidxTicket = sidx >> _shift;
                int cidxTicket = cidx >> _shift;
                if (sidxTicket != cidxTicket)
                {
                    // We are in the middle of a circular wrap-around.
                    // For simplicity we will acquire items until the wrap around happens
                    length = (_size * cidxTicket) - sidx;
                }
            }

            var items = new Span<RingItem<T>>(_buffer, (int)(sidx & _mask), length);

            // Are all items ready to be acquired?
            int i = 0;
            while (i < items.Length)
            {
                ref var item = ref items[i];
                if (Volatile.Read(ref item.IsReady))
                {
                    i++;
                }
                else
                {
                    // No, therefore we will just return what it is ready. 
                    length = i;
                    items = new Span<RingItem<T>>(_buffer, (int)(sidx & _mask), length);
                    break;
                }
            }

            // No need to use volatile here because this is Single Consumer Ring Buffer. 
            // Update to the last acquired index
            _acquiredIdx = sidx + length;

            return items;
        }

        public void Release()
        {
            Debug.Assert(_acquiredIdx != -1);

            // PERF: We can make this far more efficient that using idiv operations. 
            for (int i = Volatile.Read(ref _startIdx); i < _acquiredIdx; i++)
            {
                ref var item = ref _buffer[i & _mask];
                item.Item = default(T);
                Volatile.Write(ref item.IsReady, false);
            }

            if (_acquiredIdx != -1)
                Volatile.Write(ref _startIdx, _acquiredIdx);

            _acquiredIdx = -1;
        }
    }
}
