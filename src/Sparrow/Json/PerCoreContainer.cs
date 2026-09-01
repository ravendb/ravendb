using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;
using System.Threading;
using Sparrow.Binary;
using Sparrow.Utils;

namespace Sparrow.Json
{
    internal sealed class PerCoreContainer<T> : IEnumerable<(T Item, (int, int) Pos)>
        where T : class
    {
        private readonly int _numberOfSlotsPerCore;
        private readonly T[][] _perCoreArrays;
        private readonly PaddedInt[] _perCoreArrayLength;
        // number of per-core buckets, rounded up to a power of two so the processor id maps to a
        // bucket with a mask instead of a division; the extra buckets just start out empty
        private readonly int _coreMask;

        public PerCoreContainer(int numberOfSlotsPerCore = 64)
        {
            _numberOfSlotsPerCore = numberOfSlotsPerCore;
            var buckets = Bits.PowerOf2(Environment.ProcessorCount);
            _coreMask = buckets - 1;
            _perCoreArrays = new T[buckets][];
            _perCoreArrayLength = new PaddedInt[buckets];

            for (int i = 0; i < _perCoreArrays.Length; i++)
            {
                _perCoreArrays[i] = new T[numberOfSlotsPerCore];
            }
        }

        // Items are pulled where work runs but often pushed back from a different thread - request
        // completions funnel through the transaction merger's notification thread, so the pushes all
        // land on ONE core while the pulls happen everywhere else. If each side only ever touched its
        // own core, the pusher's slots would fill up (discarding warm items) while every puller missed
        // (rebuilding them from scratch). The local core stays the fast path; on a miss we steal from
        // the other cores, on overflow we spill to them.

        public bool TryPull(out T output)
        {
            int currentProcessorId = CurrentProcessorIdHelper.GetCurrentProcessorId() & _coreMask;

            for (int attempt = 0; attempt < _perCoreArrays.Length; attempt++)
            {
                int core = (currentProcessorId + attempt) & _coreMask;
                if (_perCoreArrayLength[core].Value <= 0)
                    continue;

                var coreItems = _perCoreArrays[core];

                for (int i = 0; i < coreItems.Length; i++)
                {
                    var cur = coreItems[i];
                    if (cur == null)
                        continue;

                    if (Interlocked.CompareExchange(ref coreItems[i], null, cur) != cur)
                        continue;

                    Interlocked.Decrement(ref _perCoreArrayLength[core].Value);
                    output = cur;
                    return true;
                }
            }

            output = default;
            return false;
        }

        public bool TryPush(T cur)
        {
            int currentProcessorId = CurrentProcessorIdHelper.GetCurrentProcessorId() & _coreMask;

            for (int attempt = 0; attempt < _perCoreArrays.Length; attempt++)
            {
                int coreId = (currentProcessorId + attempt) & _coreMask;
                if (_perCoreArrayLength[coreId].Value >= _numberOfSlotsPerCore)
                    continue;

                var core = _perCoreArrays[coreId];

                for (int i = 0; i < core.Length; i++)
                {
                    if (core[i] != null)
                        continue;

                    if (Interlocked.CompareExchange(ref core[i], cur, null) == null)
                    {
                        Interlocked.Increment(ref _perCoreArrayLength[coreId].Value);
                        return true;
                    }
                }
            }
            return false;
        }

        [SuppressMessage("ReSharper", "ForCanBeConvertedToForeach")]
        public IEnumerable<T> EnumerateAndClear()
        {
            for (var gi = 0; gi < _perCoreArrays.Length; gi++)
            {
                T[] array = _perCoreArrays[gi];
                for (int li = 0; li < array.Length; li++)
                {
                    var copy = array[li];
                    if (copy == null)
                        continue;
                    if (Interlocked.CompareExchange(ref array[li], null, copy) != copy)
                        continue;

                    Interlocked.Decrement(ref _perCoreArrayLength[gi].Value);
                    yield return copy;
                }
            }
        }

        [SuppressMessage("ReSharper", "ForCanBeConvertedToForeach")]
        public IEnumerator<(T Item, (int, int) Pos)> GetEnumerator()
        {
            for (var gi = 0; gi < _perCoreArrays.Length; gi++)
            {
                T[] array = _perCoreArrays[gi];
                for (int li = 0; li < array.Length; li++)
                {
                    var copy = array[li];
                    if (copy == null)
                        continue;
                    yield return (copy, (gi, li));
                }
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public bool Remove(T item, (int, int) pos)
        {
            var array = _perCoreArrays[pos.Item1];

            if (Interlocked.CompareExchange(ref array[pos.Item2], null, item) == item)
            {
                Interlocked.Decrement(ref _perCoreArrayLength[pos.Item1].Value);
                return true;
            }

            return false;
        }

        
    }

    [StructLayout(LayoutKind.Explicit, Size = 64)]
    internal struct PaddedInt
    {
        [FieldOffset(0)]
        public int Value;
    }
}
