using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using Sparrow.Utils;

namespace Sparrow.Json
{
    public class PerCoreContainer<T> : IEnumerable<(T Item, (int, int) Pos)>
        where T : class
    {
        private readonly T[][] _perCoreArrays;

        public PerCoreContainer(int numberOfSlotsPerCore = 64)
        {
            _perCoreArrays = new T[Environment.ProcessorCount][];
            for (int i = 0; i < _perCoreArrays.Length; i++)
            {
                _perCoreArrays[i] = new T[numberOfSlotsPerCore];
            }
        }

        public bool TryPull(out T output)
        {
            var coreItems = _perCoreArrays[CurrentProcessorIdHelper.GetCurrentProcessorId() % _perCoreArrays.Length];

            for (int i = 0; i < coreItems.Length; i++)
            {
                var cur = coreItems[i];
                if (cur == null)
                    continue;

                if (Interlocked.CompareExchange(ref coreItems[i], null, cur) != cur)
                    continue;
                output = cur;
                return true;
            }
            output = default;
            return false;
        }

        public bool TryPush(T cur)
        {
            var core = _perCoreArrays[CurrentProcessorIdHelper.GetCurrentProcessorId() % _perCoreArrays.Length];

            for (int i = 0; i < core.Length; i++)
            {
                if (core[i] != null)
                    continue;
                if (Interlocked.CompareExchange(ref core[i], cur, null) == null)
                    return true;
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
                    yield return (copy, (gi,li));
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
            return Interlocked.CompareExchange(ref array[pos.Item2], null, item) == item;
        }
    }
}
