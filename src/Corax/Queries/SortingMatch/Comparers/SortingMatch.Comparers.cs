using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;

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

        public override string ToString()
        {
            if (Size is > 0 and < 64)
                return Encoding.UTF8.GetString(Ptr, Size);
            return "Size: " + Size;
        }
    }

    internal struct NumericalItem<T> where T : unmanaged
    {
        public readonly T Value;

        public NumericalItem(in T value)
        {
            Value = value;
        }

        public override string ToString()
        {
            return Value.ToString();
        }
    }

    public readonly struct MatchComparer<T, TW> : IComparer<MatchComparer<T, TW>.Item>
        where T : IMatchComparer
        where TW : struct
    {
        public struct Item
        {
            public long Key;
            public TW Value;

            public override string ToString()
            {
                return $"{nameof(Key)}: {Key} {nameof(Value)}: {Value}";
            }
        }

        private readonly T _comparer;
        private readonly TermsReader _termsReader;

        public MatchComparer(in T comparer, TermsReader termsReader)
        {
            _comparer = comparer;
            _termsReader = termsReader;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Compare(Item ix, Item iy)
        {
            if (ix.Key > 0 && iy.Key > 0)
            {
                int cmp;
                if (typeof(TW) == typeof(SequenceItem))
                {
                    // cmp = _comparer.CompareSequence(
                    //     new ReadOnlySpan<byte>(((SequenceItem)(object)ix.Value).Ptr, ((SequenceItem)(object)ix.Value).Size),
                    //     new ReadOnlySpan<byte>(((SequenceItem)(object)iy.Value).Ptr, ((SequenceItem)(object)iy.Value).Size));
                    cmp = _termsReader.Compare(ix.Key, iy.Key);
                }
                else if (typeof(TW) == typeof(NumericalItem<long>))
                {
                    cmp = _comparer.CompareNumerical(((NumericalItem<long>)(object)ix.Value).Value, ((NumericalItem<long>)(object)iy.Value).Value);
                }
                else if (typeof(TW) == typeof(NumericalItem<double>))
                {
                    cmp = _comparer.CompareNumerical(((NumericalItem<double>)(object)ix.Value).Value, ((NumericalItem<double>)(object)iy.Value).Value);
                }
                else if (typeof(TW) == typeof(NumericalItem<float>))
                {
                    cmp = _comparer.CompareNumerical(((NumericalItem<float>)(object)ix.Value).Value, ((NumericalItem<float>)(object)iy.Value).Value);
                }
                else
                {
                    throw new NotSupportedException(typeof(TW).FullName + " is not supported");
                }

                if (cmp != 0)
                    return cmp;
                // we need to ensure that for identical values, we'll sort in the *same* order regardless of the
                // order of elements that we are observing
                return ix.Key.CompareTo(iy.Key);

            }
            else if (ix.Key > 0)
            {
                return 1;
            }

            return -1;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Compare(ref Item ix, ref Item iy)
        {
            if (ix.Key > 0 && iy.Key > 0)
            {
                int cmp;
                if (typeof(TW) == typeof(SequenceItem))
                {
                    // cmp = _comparer.CompareSequence(
                    //     new ReadOnlySpan<byte>(((SequenceItem)(object)ix.Value).Ptr, ((SequenceItem)(object)ix.Value).Size),
                    //     new ReadOnlySpan<byte>(((SequenceItem)(object)iy.Value).Ptr, ((SequenceItem)(object)iy.Value).Size));
                    cmp = _termsReader.Compare(ix.Key, iy.Key);
                }
                else if (typeof(TW) == typeof(NumericalItem<long>))
                {
                    cmp = _comparer.CompareNumerical(((NumericalItem<long>)(object)ix.Value).Value, ((NumericalItem<long>)(object)iy.Value).Value);
                }
                else if (typeof(TW) == typeof(NumericalItem<double>))
                {
                    cmp = _comparer.CompareNumerical(((NumericalItem<double>)(object)ix.Value).Value, ((NumericalItem<double>)(object)iy.Value).Value);
                }
                else if (typeof(TW) == typeof(NumericalItem<float>))
                {
                    cmp = _comparer.CompareNumerical(((NumericalItem<float>)(object)ix.Value).Value, ((NumericalItem<float>)(object)iy.Value).Value);
                }
                else
                {
                    throw new NotSupportedException(typeof(TW).FullName + " is not supported");
                }

                if (cmp != 0)
                    return cmp;
                // we need to ensure that for identical values, we'll sort in the *same* order regardless of the
                // order of elements that we are observing
                return ix.Key.CompareTo(iy.Key);
            }
            else if (ix.Key > 0)
            {
                return 1;
            }

            return -1;
        }
    }
}
