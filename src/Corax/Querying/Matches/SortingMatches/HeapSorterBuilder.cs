using System;
using System.Collections.Generic;
using Sparrow;
using Sparrow.Server;

namespace Corax.Querying.Matches.SortingMatches;

internal static class HeapSorterBuilder
{
    public static unsafe NumericalMaxHeapSorter<UnmanagedSpan, TSecondaryComparer> BuildCompoundCompactKeySorter<TSecondaryComparer>(Span<int> documents,
        Span<UnmanagedSpan> terms, bool descending, TSecondaryComparer secondaryCmp)
        where TSecondaryComparer : IComparer<int>
    {
        static int Ascending(ref NumericalMaxHeapSorter<UnmanagedSpan, TSecondaryComparer> sorter, UnmanagedSpan termA, int posA, UnmanagedSpan termB, int posB)
        {
            var cmp = CompactKeyComparer.Compare(termA, termB);
            return cmp == 0 ? 
                sorter.SecondaryComparer.Compare(posA, posB) 
                : cmp;
        }

        static int Descending(ref NumericalMaxHeapSorter<UnmanagedSpan, TSecondaryComparer> sorter, UnmanagedSpan termA, int posA, UnmanagedSpan termB, int posB)
        {
            //In first comparer we control the order of parameters, however secondary comparer (and it's inners have to be wrapped in Descending<>)
            var cmp = CompactKeyComparer.Compare(termB, termA);
            return cmp == 0 ? 
                sorter.SecondaryComparer.Compare(posA, posB) 
                : cmp;
        }

        var sorter = new NumericalMaxHeapSorter<UnmanagedSpan, TSecondaryComparer>();
        sorter.Init(documents, terms, null, descending, descending ? &Descending : &Ascending, secondaryCmp);
        return sorter;
    }


    public static unsafe TextualMaxHeapSorter<SkipSecondaryComparer> BuildSingleAlphanumericalSorter(Span<int> documents, Span<ByteString> terms,
        ByteStringContext allocator, bool descending)
    {
        static int CompareAlphanumericalAscending(ref TextualMaxHeapSorter<SkipSecondaryComparer> sorter, ReadOnlySpan<byte> termA, int posA, ReadOnlySpan<byte> termB,
            int posB)
        {
            return AlphanumericalComparer.Instance.Compare(termA, termB);
        }

        static int CompareAlphanumericalDescending(ref TextualMaxHeapSorter<SkipSecondaryComparer> sorter, ReadOnlySpan<byte> termA, int posA, ReadOnlySpan<byte> termB,
            int posB)
        {
            return AlphanumericalComparer.Instance.Compare(termB, termA);
        }

        var sorter = new TextualMaxHeapSorter<SkipSecondaryComparer>();
        sorter.Init(documents, terms, allocator, descending, descending ? &CompareAlphanumericalDescending : &CompareAlphanumericalAscending, default);
        return sorter;
    }

    public static unsafe TextualMaxHeapSorter<TSecondaryCmp> BuildCompoundAlphanumericalSorter<TSecondaryCmp>(Span<int> documents, Span<ByteString> terms,
        ByteStringContext allocator, bool descending, TSecondaryCmp secondaryCmp) where TSecondaryCmp : IComparer<int>
    {
        static int CompareAlphanumericalAscending(ref TextualMaxHeapSorter<TSecondaryCmp> sorter, ReadOnlySpan<byte> termA, int posA, ReadOnlySpan<byte> termB, int posB)
        {
            var result = AlphanumericalComparer.Instance.Compare(termA, termB);
            return result == 0 ? sorter.SecondaryComparer.Compare(posA, posB) : result;
        }

        static int CompareAlphanumericalDescending(ref TextualMaxHeapSorter<TSecondaryCmp> sorter, ReadOnlySpan<byte> termA, int posA, ReadOnlySpan<byte> termB, int posB)
        {
            //note reversed elements, only for the first comparer, inner comparers should be wrapped in Descending<T>
            var result = AlphanumericalComparer.Instance.Compare(termB, termA);
            return result == 0 ? sorter.SecondaryComparer.Compare(posA, posB) : result;
        }

        var sorter = new TextualMaxHeapSorter<TSecondaryCmp>();
        sorter.Init(documents, terms, allocator, descending, descending ? &CompareAlphanumericalDescending : &CompareAlphanumericalAscending, secondaryCmp);
        return sorter;
    }

    public static unsafe NumericalMaxHeapSorter<TTermType, SkipSecondaryComparer> BuildSingleNumericalSorter<TTermType>(Span<int> documents, Span<TTermType> terms,
        bool descending) where TTermType : unmanaged, IComparable
    {
        static int Ascending(ref NumericalMaxHeapSorter<TTermType, SkipSecondaryComparer> sorter, TTermType termA, int posA, TTermType termB, int posB)
        {
            return termA.CompareTo(termB);
        }

        static int Descending(ref NumericalMaxHeapSorter<TTermType, SkipSecondaryComparer> sorter, TTermType termA, int posA, TTermType termB, int posB)
        {
            return termB.CompareTo(termA);
        }

        var sorter = new NumericalMaxHeapSorter<TTermType, SkipSecondaryComparer>();
        sorter.Init(documents, terms, null, descending, descending ? &Descending : &Ascending, default);
        return sorter;
    }

    public static unsafe NumericalMaxHeapSorter<TTermType, TSecondaryCmp> BuildCompoundNumericalSorter<TTermType, TSecondaryCmp>(Span<int> documents,
        Span<TTermType> terms, bool descending, TSecondaryCmp secondaryCmp)
        where TSecondaryCmp : IComparer<int>
        where TTermType : unmanaged, IComparable
    {
        static int Ascending(ref NumericalMaxHeapSorter<TTermType, TSecondaryCmp> sorter, TTermType termA, int posA, TTermType termB, int posB)
        {
            var cmp = termA.CompareTo(termB);
            return cmp == 0
                ? sorter.SecondaryComparer.Compare(posA, posB)
                : cmp;
        }

        static int Descending(ref NumericalMaxHeapSorter<TTermType, TSecondaryCmp> sorter, TTermType termA, int posA, TTermType termB, int posB)
        {
            var cmp = termB.CompareTo(termA);
            return cmp == 0
                ? sorter.SecondaryComparer.Compare(posA, posB)
                : cmp;
        }

        var sorter = new NumericalMaxHeapSorter<TTermType, TSecondaryCmp>();
        sorter.Init(documents, terms, null, descending, descending ? &Descending : &Ascending, secondaryCmp);
        return sorter;
    }

    internal struct SkipSecondaryComparer : IComparer<int>
    {
        public int Compare(int x, int y)
        {
            throw new NotImplementedException("Used as marker for generics. Should never ever be called!");
        }
    }
}
