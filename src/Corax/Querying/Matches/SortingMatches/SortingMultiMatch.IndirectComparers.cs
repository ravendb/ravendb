using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Corax.Querying.Matches.SortingMatches.Meta;
using Corax.Utils;
using Sparrow;

namespace Corax.Querying.Matches.SortingMatches;

public unsafe partial struct SortingMultiMatch<TInner>
{
    //This is used as second degree comparer for alphanumerical
    private readonly struct IndirectComparer2<TComparer2, TComparer3> : IComparer<int>
        where TComparer2 : struct, IComparer<int>, IComparer<UnmanagedSpan>
        where TComparer3 : struct, IComparer<int>, IComparer<UnmanagedSpan>
    {
        private readonly TComparer2 _cmp2;
        private readonly TComparer3 _cmp3;
        private readonly IEntryComparer[] _nextComparers;
        private readonly int _maxDegreeOfInnerComparer;

        public IndirectComparer2(ref SortingMultiMatch<TInner> match, TComparer2 cmp2, TComparer3 cmp3)
        {
            _cmp2 = cmp2;
            _cmp3 = cmp3;
            _nextComparers = match._nextComparers;

            _maxDegreeOfInnerComparer += typeof(TComparer2) == typeof(NullComparer) ? 0 : 1;
            _maxDegreeOfInnerComparer += typeof(TComparer3) == typeof(NullComparer) ? 0 : 1;
            _maxDegreeOfInnerComparer += _nextComparers.Length;
        }
        
        public int Compare(int x, int y)
        {
            var cmp = 0;
            for (int comparerId = 0; cmp == 0 && comparerId < _maxDegreeOfInnerComparer; ++comparerId)
            {
                cmp = comparerId switch
                {
                    0 => _cmp2.Compare(x, y),
                    1 => _cmp3.Compare(x, y),
                    _ => _nextComparers[comparerId - 2].Compare(x, y)
                };
            }

            return cmp;
        }
    }

private readonly struct IndirectComparer<TComparer1, TComparer2, TComparer3> : IComparer<long>, IComparer<int>
        where TComparer1 : struct, IComparer<UnmanagedSpan>
        where TComparer2 : struct, IComparer<int>, IComparer<UnmanagedSpan>
        where TComparer3 : struct, IComparer<int>, IComparer<UnmanagedSpan>
    {
        private readonly UnmanagedSpan* _terms;
        private readonly TComparer1 _cmp1;
        private readonly TComparer2 _cmp2;
        private readonly TComparer3 _cmp3;
        private readonly IEntryComparer[] _nextComparers;
        private readonly int _maxDegreeOfInnerComparer;

        public IndirectComparer(ref SortingMultiMatch<TInner> match, UnmanagedSpan* terms, TComparer1 entryComparer, TComparer2 cmp2, TComparer3 cmp3)
        {
            _terms = terms;
            _cmp1 = entryComparer;
            _cmp2 = cmp2;
            _cmp3 = cmp3;
            _nextComparers = match._nextComparers;

            if (typeof(TComparer1) == typeof(NullComparer))
                _maxDegreeOfInnerComparer = 0;
            else if (typeof(TComparer2) == typeof(NullComparer))
                _maxDegreeOfInnerComparer = 1;
            else if (typeof(TComparer3) == typeof(NullComparer))
                _maxDegreeOfInnerComparer = 2;
            else
                _maxDegreeOfInnerComparer = 3;
            
            _maxDegreeOfInnerComparer += _nextComparers.Length;
        }

        public int Compare(long x, long y)
        {
            var xIdx = (ushort)x & 0X7FFF;
            var yIdx = (ushort)y & 0X7FFF;

            Debug.Assert(yIdx < SortingMatch.SortBatchSize && xIdx < SortingMatch.SortBatchSize);

            var cmp = 0;
            for (int comparerId = 0; cmp == 0 && comparerId < _maxDegreeOfInnerComparer; ++comparerId)
            {
                cmp = comparerId switch
                {
                    0 => _cmp1.Compare(_terms[xIdx], _terms[yIdx]),
                    1 => _cmp2.Compare(xIdx, yIdx),
                    2 => _cmp3.Compare(xIdx, yIdx),
                    _ => _nextComparers[comparerId - 3].Compare(xIdx, yIdx)
                };
            }
            
            return cmp;
        }


        public int Compare(int x, int y)
        {
            var cmp = 0;
            for (int comparerId = 0; cmp == 0 && comparerId < _maxDegreeOfInnerComparer; ++comparerId)
            {
                cmp = comparerId switch
                {
                    0 => _cmp1.Compare(_terms[x], _terms[y]),
                    1 => _cmp2.Compare(x, y),
                    2 => _cmp3.Compare(x, y),
                    _ => _nextComparers[comparerId - 3].Compare(x, y)
                };
            }

            return cmp;
        }
    }
}
