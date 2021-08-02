using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace Corax.Queries
{
    public enum MatchCompareFieldType
    {
        Sequence,
        Integer,
        Floating
    }

    public interface IMatchComparer : IComparer<long> { }

    partial struct SortingMatch
    {                     
        private static class BasicComparers
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal static int CompareAsSequence(IndexSearcher searcher, int fieldId, long x, long y)
            {
                var readerX = searcher.GetReaderFor(x);
                var readX = readerX.Read(fieldId, out var resultX);

                var readerY = searcher.GetReaderFor(y);
                var readY = readerY.Read(fieldId, out var resultY);

                // TODO: sort by asc, desc, multipel fields
                // sort by long, sort by double, sort by lexical, sort by random, sort by alphanumeric
                // sort by score()

                if (readX && readY)
                    return resultX.SequenceCompareTo(resultY);
                else if (readX)
                    return 1;
                return -1;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal static int CompareAsInteger(IndexSearcher searcher, int fieldId, long x, long y)
            {
                var readerX = searcher.GetReaderFor(x);
                var readX = readerX.Read<long>(fieldId, out var resultX);

                var readerY = searcher.GetReaderFor(y);
                var readY = readerY.Read<long>(fieldId, out var resultY);

                if (readX && readY)
                    return Math.Sign(resultY - resultX);
                else if (readX)
                    return 1;
                return -1;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal static int CompareAsDouble(IndexSearcher searcher, int fieldId, long x, long y)
            {
                var readerX = searcher.GetReaderFor(x);
                var readX = readerX.Read<double>(fieldId, out var resultX);

                var readerY = searcher.GetReaderFor(y);
                var readY = readerY.Read<double>(fieldId, out var resultY);

                if (readX && readY)
                    return Math.Sign(resultY - resultX);
                else if (readX)
                    return 1;
                return -1;
            }
        }

        public unsafe struct CustomMatchComparer : IMatchComparer
        {
            private readonly IndexSearcher _searcher;
            private readonly int _fieldId;
            private readonly delegate*<IndexSearcher, int, long, long, int> _compareFunc;

            public CustomMatchComparer(IndexSearcher searcher, int fieldId, delegate*<IndexSearcher, int, long, long, int> compareFunc)
            {
                _searcher = searcher;
                _fieldId = fieldId;
                _compareFunc = compareFunc;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public int Compare(long x, long y)
            {
                return _compareFunc(_searcher, _fieldId, x, y);
            }
        }

        public unsafe struct AscendingMatchComparer : IMatchComparer
        {
            private readonly IndexSearcher _searcher;
            private readonly int _fieldId;
            private readonly delegate*<IndexSearcher, int, long, long, int> _compareFunc;

            public AscendingMatchComparer(IndexSearcher searcher, int fieldId, MatchCompareFieldType entryFieldType)
            {
                _searcher = searcher;
                _fieldId = fieldId;
                _compareFunc = entryFieldType switch
                {
                    MatchCompareFieldType.Sequence => &BasicComparers.CompareAsSequence,
                    MatchCompareFieldType.Integer => &BasicComparers.CompareAsInteger,
                    MatchCompareFieldType.Floating => &BasicComparers.CompareAsDouble,
                };
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public int Compare(long x, long y)
            {
                return _compareFunc(_searcher, _fieldId, x, y);
            }
        }

        public unsafe struct DescendingMatchComparer : IMatchComparer
        {
            private readonly IndexSearcher _searcher;
            private readonly int _fieldId;
            private readonly delegate*<IndexSearcher, int, long, long, int> _compareFunc;

            public DescendingMatchComparer(IndexSearcher searcher, int fieldId, MatchCompareFieldType entryFieldType)
            {
                _searcher = searcher;
                _fieldId = fieldId;
                _compareFunc = entryFieldType switch
                {
                    MatchCompareFieldType.Sequence => &BasicComparers.CompareAsSequence,
                    MatchCompareFieldType.Integer => &BasicComparers.CompareAsInteger,
                    MatchCompareFieldType.Floating => &BasicComparers.CompareAsDouble,
                };
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public int Compare(long x, long y)
            {
                return - _compareFunc(_searcher, _fieldId, x, y);
            }
        }
    }
}
