using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Corax.Queries;

unsafe partial struct SortingMatch
{
    internal struct SequenceItem
    {
        public readonly byte* Ptr;
        public readonly int Size;

        public SequenceItem(byte* ptr, int size)
        {
            Ptr = ptr;
            Size = size;
        }
    }

    internal struct NumericalItem<T> where T : unmanaged
    {
        public readonly T Value;

        public NumericalItem(in T value)
        {
            Value = value;
        }
    }

    internal struct MatchComparer<T, W> : IComparer<MatchComparer<T, W>.Item>
        where T : IMatchComparer
        where W : struct
    {
        public struct Item
        {
            public long Key;
            public W Value;
        }

        private readonly T _comparer;

        public MatchComparer(in T comparer)
        {
            _comparer = comparer;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Compare(Item ix, Item iy)
        {
            if (ix.Key > 0 && iy.Key > 0)
            {
                if (typeof(W) == typeof(SequenceItem))
                {
                    return _comparer.CompareSequence(
                        new ReadOnlySpan<byte>(((SequenceItem)(object)ix.Value).Ptr, ((SequenceItem)(object)ix.Value).Size),
                        new ReadOnlySpan<byte>(((SequenceItem)(object)iy.Value).Ptr, ((SequenceItem)(object)iy.Value).Size));
                }
                else if (typeof(W) == typeof(NumericalItem<long>))
                {
                    return _comparer.CompareNumerical(((NumericalItem<long>)(object)ix.Value).Value, ((NumericalItem<long>)(object)iy.Value).Value);
                }
                else if (typeof(W) == typeof(NumericalItem<double>))
                {
                    return _comparer.CompareNumerical(((NumericalItem<double>)(object)ix.Value).Value, ((NumericalItem<double>)(object)iy.Value).Value);
                }
            }
            else if (ix.Key > 0)
            {
                return 1;
            }

            return -1;
        }
    }
}
