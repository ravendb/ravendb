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
using Voron.Data.Lookups;

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
        private readonly CompactTree _tree;
        private readonly IndexSearcher _searcher;
        private readonly FieldMetadata _field;
        private readonly Slice _low, _high;

        private CompactTree.ForwardIterator _iterator;

        private readonly bool _skipHighCheck;
        private bool _skipLowCheck;

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

        public bool Next(out TermMatch term, out CompactKeyCacheScope termScope)
        {
            if (_iterator.MoveNext(out termScope, out var _) == false)
            {
                term = TermMatch.CreateEmpty(_searcher, _searcher.Allocator);
                return false;
            }

            var termSlice = termScope.Key.Decoded();

            if (typeof(TLow) == typeof(Range.Exclusive))
            {
                if (_skipLowCheck)
                {
                    _skipLowCheck = false;
                    if (_low.AsSpan().SequenceEqual(termSlice))
                    {
                        return Next(out term, out termScope);
                    }
                }
            }

            if (_skipHighCheck == false)
            {
                int cmp = _high.AsSpan().SequenceCompareTo(termSlice);
                if (typeof(THigh) == typeof(Range.Exclusive) && cmp <= 0 || 
                    typeof(THigh) == typeof(Range.Inclusive) && cmp < 0)
                {
                    term = TermMatch.CreateEmpty(_searcher, _searcher.Allocator);
                    return false;
                }

            }
            
            term = _searcher.TermQuery(_field, termScope.Key, _tree);
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
        private Lookup<TVal>.ForwardIterator _iterator;
        private bool _first;

        public TermNumericRangeProvider(IndexSearcher searcher, Lookup<TVal> set, FieldMetadata field, TVal low, TVal high)
        {
            _searcher = searcher;
            _field = field;
            _iterator = set.Iterate();
            _low = low;
            _high = high;
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
                _iterator.Seek(_low);
                _first = false;
                wasFirst = true;
            }

            hasNext = _iterator.MoveNext(out var key, out var termId);

            if (hasNext == false)
                goto Empty;
            
            if (typeof(TLow) == typeof(Range.Exclusive))
            {
                if (wasFirst && key == _low)
                {
                    return Next(out term);
                }
            }     


            var cmp = _high.CompareTo(key);
            if (typeof(THigh) == typeof(Range.Exclusive) && cmp <= 0 || 
                typeof(THigh) == typeof(Range.Inclusive) && cmp < 0)
            {
                goto Empty;
            }
            
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
