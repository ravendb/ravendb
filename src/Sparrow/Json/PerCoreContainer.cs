using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using Sparrow.Utils;

namespace Sparrow.Json
{
    internal sealed class PerCoreContainer<T> : IEnumerable<(T Item, (int, int) Pos)>
        where T : class
    {
        private readonly int _numberOfSlotsPerCore;
        private readonly T[][] _perCoreArrays;
        private readonly int[] _perCoreArrayLength;

        public PerCoreContainer(int numberOfSlotsPerCore = 64)
        {
            _numberOfSlotsPerCore = numberOfSlotsPerCore;
            _perCoreArrays = new T[Environment.ProcessorCount][];
            _perCoreArrayLength = new int[Environment.ProcessorCount];

            for (int i = 0; i < _perCoreArrays.Length; i++)
            {
                _perCoreArrays[i] = new T[numberOfSlotsPerCore];
            }
        }

        public bool TryPull(out T output)
        {
            int currentProcessorId = CurrentProcessorIdHelper.GetCurrentProcessorId() % _perCoreArrays.Length;
            if (_perCoreArrayLength[currentProcessorId] <= 0)
            {
                output = default;
                return false;
            }

            var coreItems = _perCoreArrays[currentProcessorId];

            for (int i = 0; i < coreItems.Length; i++)
            {
                var cur = coreItems[i];
                if (cur == null)
                    continue;

                if (Interlocked.CompareExchange(ref coreItems[i], null, cur) != cur)
                    continue;

                Interlocked.Decrement(ref _perCoreArrayLength[currentProcessorId]);
                output = cur;
                return true;
            }

            output = default;
            return false;
        }

        public bool TryPush(T cur)
        {
            int currentProcessorId = CurrentProcessorIdHelper.GetCurrentProcessorId() % _perCoreArrays.Length;
            if (_perCoreArrayLength[currentProcessorId] >= _numberOfSlotsPerCore)
                return false;

            var core = _perCoreArrays[currentProcessorId];

            for (int i = 0; i < core.Length; i++)
            {
                if (core[i] != null)
                    continue;

                if (Interlocked.CompareExchange(ref core[i], cur, null) == null)
                {
                    Interlocked.Increment(ref _perCoreArrayLength[currentProcessorId]);
                    return true;
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

                    Interlocked.Decrement(ref _perCoreArrayLength[gi]);
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
                Interlocked.Decrement(ref _perCoreArrayLength[pos.Item1]);
                return true;
            }

            return false;
        }
    }
}
