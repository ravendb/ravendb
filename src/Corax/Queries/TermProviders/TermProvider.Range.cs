using System;
using System.Buffers;
using System.Buffers.Text;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using Corax.Mappings;
using Sparrow.Server;
using Voron;
using Voron.Data.CompactTrees;
using Voron.Data.Fixed;

namespace Corax.Queries
{
    public static class Range
    {
        public interface Marker { }
        public struct Exclusive : Marker { }
        public struct Inclusive : Marker { }
    }

    [DebuggerDisplay("{DebugView,nq}")]
    public struct TermRangeProvider<TLow, THigh> : ITermProvider
        where TLow : struct, Range.Marker
        where THigh  : struct, Range.Marker
    {
        private readonly IndexSearcher _searcher;
        private readonly FieldMetadata _field;
        private readonly Slice _low, _high;
        private readonly CompactTree _tree;
        private CompactTree.Iterator _iterator;
        private bool _skipLowCheck;
        private readonly bool _skipHighCheck;

        public TermRangeProvider(IndexSearcher searcher, CompactTree tree, FieldMetadata field, Slice low, Slice high)
        {
            _searcher = searcher;
            _field = field;
            _iterator = tree.Iterate();
            _low = low;
            _high = high;
            _tree = tree;
            _skipLowCheck = false;
            _skipHighCheck = high.Options == SliceOptions.AfterAllKeys;
            Reset();
        }

        public void Reset()
        {
            if (_low.Options != SliceOptions.BeforeAllKeys)
            {
                _iterator.Seek(_low);
                _skipLowCheck = typeof(TLow) == typeof(Range.Exclusive);
            }
            else
            {
                _iterator.Reset();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Next(out TermMatch term) => Next(out term, out _);

        public bool Next(out TermMatch term, out Slice termSlice)
        {
            if (_iterator.MoveNext(out termSlice, out var _) == false)
            {
                term = TermMatch.CreateEmpty(_searcher, _searcher.Allocator);
                return false;
            }

            if (typeof(TLow) == typeof(Range.Exclusive))
            {
                if (_skipLowCheck)
                {
                    _skipLowCheck = false;
                    if (_low.AsSpan().SequenceEqual(termSlice.AsSpan()))
                    {
                        return Next(out term, out termSlice);
                    }
                }
            }

            if (_skipHighCheck == false)
            {
                int cmp = _high.AsSpan().SequenceCompareTo(termSlice.AsSpan());
                if (typeof(THigh) == typeof(Range.Exclusive) && cmp <= 0 || 
                    typeof(THigh) == typeof(Range.Inclusive) && cmp < 0)
                {
                    term = TermMatch.CreateEmpty(_searcher, _searcher.Allocator);
                    return false;
                }

            }
            
            term = _searcher.TermQuery(_field, _tree, termSlice);
            return true;
        }

        public QueryInspectionNode Inspect()
        {
            return new QueryInspectionNode($"{GetType().Name}",
                            parameters: new Dictionary<string, string>()
                            {
                                { "Field", _field.ToString() },
                                { "Low", _low.ToString()},
                                { "High", _high.ToString()}
                            });
        }

        public string DebugView => Inspect().ToString();
    }
    
     [DebuggerDisplay("{DebugView,nq}")]
    public unsafe struct TermNumericRangeProvider<TLow, THigh, TVal> : ITermProvider
        where TLow : struct, Range.Marker
        where THigh  : struct, Range.Marker
        where TVal : unmanaged, IBinaryNumber<TVal>, IMinMaxValue<TVal>, INumber<TVal>
    {
        private readonly IndexSearcher _searcher;
        private readonly FieldMetadata _field;
        private readonly TVal _low, _high;
        private readonly CompactTree _tree;
        private readonly FixedSizeTree<TVal> _set;
        private readonly FixedSizeTree<TVal>.IFixedSizeIterator _iterator;
        private bool _first;

        private const int TermBufferSize = 32;
        private fixed byte _termsBuffer[TermBufferSize];

        public TermNumericRangeProvider(IndexSearcher searcher, FixedSizeTree<TVal> set,
            CompactTree tree, FieldMetadata field, TVal low, TVal high)
        {
            _searcher = searcher;
            _field = field;
            _iterator = set.Iterate();
            _low = low;
            _high = high;
            _set = set;
            _tree = tree;
            _first = true;
        }

        public void Reset()
        {
            _first = true;
        }
        
        public bool Next(out TermMatch term)
        {
            bool wasFirst = false;
            bool hasNext;
            if (_first)
            {
                hasNext = _iterator.Seek(_low);
                _first = false;
                wasFirst = true;
            }
            else
            {
                hasNext = _iterator.MoveNext();
            }

            if (hasNext == false)
                goto Empty;
            
            var curVal = _iterator.CurrentKey;
            if (typeof(TLow) == typeof(Range.Exclusive))
            {
                if (wasFirst && curVal == _low)
                {
                    return Next(out term);
                }
            }     


            var cmp = _high.CompareTo(curVal);
            if (typeof(THigh) == typeof(Range.Exclusive) && cmp <= 0 || 
                typeof(THigh) == typeof(Range.Inclusive) && cmp < 0)
            {
                goto Empty;
            }
            
            var termId = _iterator.CreateReaderForCurrent().ReadLittleEndianInt64();
            // Ratio will be always 1 (sizeof(T)/sizeof(T))
            term = _searcher.TermQuery(_field, termId, 1);
            return true;

            Empty:
            term = TermMatch.CreateEmpty(_searcher, _searcher.Allocator);
            return false;
        }

        public QueryInspectionNode Inspect()
        {
            return new QueryInspectionNode($"{GetType().Name}",
                            parameters: new Dictionary<string, string>()
                            {
                                { "Field", _field.ToString() },
                                { "Low", _low.ToString()},
                                { "High", _high.ToString()}
                            });
        }

        public string DebugView => Inspect().ToString();
    }
}
