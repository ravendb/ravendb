using System;
using System.Runtime.CompilerServices;

namespace Corax.Queries
{
    public unsafe partial struct SortingMultiMatch : IQueryMatch
    {
        private readonly FunctionTable _functionTable;
        private IQueryMatch _inner;

        internal SortingMultiMatch(IQueryMatch match, FunctionTable functionTable)
        {
            _inner = match;
            _functionTable = functionTable;
        }

        public long TotalResults => _functionTable.TotalResultsFunc(ref this);

        public long Count => throw new NotSupportedException();

        public QueryCountConfidence Confidence => throw new NotSupportedException();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Fill(Span<long> buffer)
        {
            return _functionTable.FillFunc(ref this, buffer);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int AndWith(Span<long> buffer)
        {
            throw new NotSupportedException($"{nameof(SortingMatch)} does not support the operation of {nameof(AndWith)}.");
        }

        internal class FunctionTable
        {
            public readonly delegate*<ref SortingMultiMatch, Span<long>, int> FillFunc;
            public readonly delegate*<ref SortingMultiMatch, long> TotalResultsFunc;

            public FunctionTable(
                delegate*<ref SortingMultiMatch, Span<long>, int> fillFunc,
                delegate*<ref SortingMultiMatch, long> totalResultsFunc)
            {
                FillFunc = fillFunc;
                TotalResultsFunc = totalResultsFunc;
            }
        }

        public struct NullComparer : IMatchComparer
        {
            public MatchCompareFieldType FieldType => throw new NotSupportedException();

            public int FieldId => throw new NotSupportedException();

            public int CompareById(long idx, long idy) { throw new NotSupportedException(); }

            public int CompareNumerical<T>(T sx, T sy) where T : unmanaged { throw new NotSupportedException(); }

            public int CompareSequence(ReadOnlySpan<byte> sx, ReadOnlySpan<byte> sy) { throw new NotSupportedException(); }
        }

        private static class StaticFunctionCache<TInner, TComparer1, TComparer2, TComparer3, TComparer4, TComparer5, TComparer6, TComparer7, TComparer8, TComparer9>
            where TInner : IQueryMatch
            where TComparer1 : struct, IMatchComparer
            where TComparer2 : struct, IMatchComparer
            where TComparer3 : struct, IMatchComparer
            where TComparer4 : struct, IMatchComparer
            where TComparer5 : struct, IMatchComparer
            where TComparer6 : struct, IMatchComparer
            where TComparer7 : struct, IMatchComparer
            where TComparer8 : struct, IMatchComparer
            where TComparer9 : struct, IMatchComparer
        {
            public static readonly FunctionTable FunctionTable;

            static StaticFunctionCache()
            {
                static long CountFunc(ref SortingMultiMatch match)
                {
                    return ((SortingMultiMatch<TInner, TComparer1, TComparer2, TComparer3, TComparer4, TComparer5, TComparer6, TComparer7, TComparer8, TComparer9>)match._inner).TotalResults;
                }
                static int FillFunc(ref SortingMultiMatch match, Span<long> matches)
                {
                    if (match._inner is SortingMultiMatch<TInner, TComparer1, TComparer2, TComparer3, TComparer4, TComparer5, TComparer6, TComparer7, TComparer8, TComparer9> inner)
                    {
                        var result = inner.Fill(matches);
                        match._inner = inner;
                        return result;
                    }
                    return 0;
                }

                FunctionTable = new FunctionTable(&FillFunc, &CountFunc);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static SortingMultiMatch Create<TInner, TComparer1, TComparer2, TComparer3, TComparer4, TComparer5, TComparer6, TComparer7, TComparer8, TComparer9>(
            in SortingMultiMatch<TInner, TComparer1, TComparer2, TComparer3, TComparer4, TComparer5, TComparer6, TComparer7, TComparer8, TComparer9> query)
            where TInner : IQueryMatch
            where TComparer1 : struct, IMatchComparer
            where TComparer2 : struct, IMatchComparer
            where TComparer3 : struct, IMatchComparer
            where TComparer4 : struct, IMatchComparer
            where TComparer5 : struct, IMatchComparer
            where TComparer6 : struct, IMatchComparer
            where TComparer7 : struct, IMatchComparer
            where TComparer8 : struct, IMatchComparer
            where TComparer9 : struct, IMatchComparer
        {
            return new SortingMultiMatch(query,
                StaticFunctionCache<TInner,
                TComparer1, TComparer2, TComparer3,
                TComparer4, TComparer5, TComparer6,
                TComparer7, TComparer8, TComparer9>.FunctionTable);
        }

        public static SortingMultiMatch Create<TInner, TComparer1, TComparer2, TComparer3, TComparer4, TComparer5, TComparer6, TComparer7, TComparer8, TComparer9>(
            IndexSearcher searcher, TInner inner,
            TComparer1 comparer1, TComparer2 comparer2, TComparer3 comparer3,
            TComparer4 comparer4, TComparer5 comparer5, TComparer6 comparer6,
            TComparer7 comparer7, TComparer8 comparer8, TComparer9 comparer9
            )
            where TInner : IQueryMatch
            where TComparer1 : struct, IMatchComparer
            where TComparer2 : struct, IMatchComparer
            where TComparer3 : struct, IMatchComparer
            where TComparer4 : struct, IMatchComparer
            where TComparer5 : struct, IMatchComparer
            where TComparer6 : struct, IMatchComparer
            where TComparer7 : struct, IMatchComparer
            where TComparer8 : struct, IMatchComparer
            where TComparer9 : struct, IMatchComparer
        {
            return Create(
                new SortingMultiMatch<TInner, TComparer1, TComparer2, TComparer3, TComparer4, TComparer5, TComparer6, TComparer7, TComparer8, TComparer9>(
                    searcher, inner,
                    comparer1, comparer2, comparer3,
                    comparer4, comparer5, comparer6,
                    comparer7, comparer8, comparer9));
        }

        public static SortingMultiMatch Create<TInner, TComparer1, TComparer2, TComparer3, TComparer4, TComparer5, TComparer6, TComparer7, TComparer8>(
            IndexSearcher searcher, TInner inner,
            TComparer1 comparer1, TComparer2 comparer2, TComparer3 comparer3,
            TComparer4 comparer4, TComparer5 comparer5, TComparer6 comparer6,
            TComparer7 comparer7, TComparer8 comparer8)
            where TInner : IQueryMatch
            where TComparer1 : struct, IMatchComparer
            where TComparer2 : struct, IMatchComparer
            where TComparer3 : struct, IMatchComparer
            where TComparer4 : struct, IMatchComparer
            where TComparer5 : struct, IMatchComparer
            where TComparer6 : struct, IMatchComparer
            where TComparer7 : struct, IMatchComparer
            where TComparer8 : struct, IMatchComparer
        {
            return Create(
                new SortingMultiMatch<TInner, TComparer1, TComparer2, TComparer3, TComparer4, TComparer5, TComparer6, TComparer7, TComparer8, NullComparer>(
                    searcher, inner,
                    comparer1, comparer2, comparer3,
                    comparer4, comparer5, comparer6,
                    comparer7, comparer8));
        }

        public static SortingMultiMatch Create<TInner, TComparer1, TComparer2, TComparer3, TComparer4, TComparer5, TComparer6, TComparer7>(
            IndexSearcher searcher, TInner inner,
            TComparer1 comparer1, TComparer2 comparer2, TComparer3 comparer3,
            TComparer4 comparer4, TComparer5 comparer5, TComparer6 comparer6,
            TComparer7 comparer7)
            where TInner : IQueryMatch
            where TComparer1 : struct, IMatchComparer
            where TComparer2 : struct, IMatchComparer
            where TComparer3 : struct, IMatchComparer
            where TComparer4 : struct, IMatchComparer
            where TComparer5 : struct, IMatchComparer
            where TComparer6 : struct, IMatchComparer
            where TComparer7 : struct, IMatchComparer
        {
            return Create(
                new SortingMultiMatch<TInner, TComparer1, TComparer2, TComparer3, TComparer4, TComparer5, TComparer6, TComparer7, NullComparer, NullComparer>(
                    searcher, inner,
                    comparer1, comparer2, comparer3,
                    comparer4, comparer5, comparer6,
                    comparer7));
        }

        public static SortingMultiMatch Create<TInner, TComparer1, TComparer2, TComparer3, TComparer4, TComparer5, TComparer6>(
            IndexSearcher searcher, TInner inner,
            TComparer1 comparer1, TComparer2 comparer2, TComparer3 comparer3,
            TComparer4 comparer4, TComparer5 comparer5, TComparer6 comparer6)
            where TInner : IQueryMatch
            where TComparer1 : struct, IMatchComparer
            where TComparer2 : struct, IMatchComparer
            where TComparer3 : struct, IMatchComparer
            where TComparer4 : struct, IMatchComparer
            where TComparer5 : struct, IMatchComparer
            where TComparer6 : struct, IMatchComparer
        {
            return Create(
                new SortingMultiMatch<TInner, TComparer1, TComparer2, TComparer3, TComparer4, TComparer5, TComparer6, NullComparer, NullComparer, NullComparer>(
                    searcher, inner,
                    comparer1, comparer2, comparer3,
                    comparer4, comparer5, comparer6));
        }

        public static SortingMultiMatch Create<TInner, TComparer1, TComparer2, TComparer3, TComparer4, TComparer5>(
            IndexSearcher searcher, TInner inner,
            TComparer1 comparer1, TComparer2 comparer2, TComparer3 comparer3,
            TComparer4 comparer4, TComparer5 comparer5)
            where TInner : IQueryMatch
            where TComparer1 : struct, IMatchComparer
            where TComparer2 : struct, IMatchComparer
            where TComparer3 : struct, IMatchComparer
            where TComparer4 : struct, IMatchComparer
            where TComparer5 : struct, IMatchComparer
        {
            return Create(
                new SortingMultiMatch<TInner, TComparer1, TComparer2, TComparer3, TComparer4, TComparer5, NullComparer, NullComparer, NullComparer, NullComparer>(
                    searcher, inner,
                    comparer1, comparer2, comparer3,
                    comparer4, comparer5));
        }

        public static SortingMultiMatch Create<TInner, TComparer1, TComparer2, TComparer3, TComparer4>(
            IndexSearcher searcher, TInner inner,
            TComparer1 comparer1, TComparer2 comparer2, TComparer3 comparer3,
            TComparer4 comparer4)
            where TInner : IQueryMatch
            where TComparer1 : struct, IMatchComparer
            where TComparer2 : struct, IMatchComparer
            where TComparer3 : struct, IMatchComparer
            where TComparer4 : struct, IMatchComparer
        {
            return Create(
                new SortingMultiMatch<TInner, TComparer1, TComparer2, TComparer3, TComparer4, NullComparer, NullComparer, NullComparer, NullComparer, NullComparer>(
                    searcher, inner,
                    comparer1, comparer2, comparer3,
                    comparer4));
        }

        public static SortingMultiMatch Create<TInner, TComparer1, TComparer2, TComparer3>(
            IndexSearcher searcher, TInner inner,
            TComparer1 comparer1, TComparer2 comparer2, TComparer3 comparer3)
            where TInner : IQueryMatch
            where TComparer1 : struct, IMatchComparer
            where TComparer2 : struct, IMatchComparer
            where TComparer3 : struct, IMatchComparer
        {
            return Create(
                new SortingMultiMatch<TInner, TComparer1, TComparer2, TComparer3, NullComparer, NullComparer, NullComparer, NullComparer, NullComparer, NullComparer>(
                    searcher, inner,
                    comparer1, comparer2, comparer3));
        }

        public static SortingMultiMatch Create<TInner, TComparer1, TComparer2>(
            IndexSearcher searcher, TInner inner,
            TComparer1 comparer1, TComparer2 comparer2)
            where TInner : IQueryMatch
            where TComparer1 : struct, IMatchComparer
            where TComparer2 : struct, IMatchComparer
        {
            return Create(
                new SortingMultiMatch<TInner, TComparer1, TComparer2, NullComparer, NullComparer, NullComparer, NullComparer, NullComparer, NullComparer, NullComparer>(
                    searcher, inner,
                    comparer1, comparer2));
        }
    }
}
