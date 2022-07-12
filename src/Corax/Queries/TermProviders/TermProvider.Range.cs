using System;
using System.Buffers;
using System.Buffers.Text;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
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
        private readonly Slice _fieldName;
        private readonly Slice _low, _high;
        private readonly CompactTree _tree;
        private CompactTree.Iterator _iterator;
        private bool _skipLowCheck;
        private readonly bool _skipHighCheck;

        public TermRangeProvider(IndexSearcher searcher, CompactTree tree, Slice fieldName, 
            Slice low, Slice high)
        {
            _searcher = searcher;
            _fieldName = fieldName;
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
                term = TermMatch.CreateEmpty();
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
                    term = TermMatch.CreateEmpty();
                    return false;
                }

            }
            
            term = _searcher.TermQuery(_tree, termSlice);
            return true;
        }

        public QueryInspectionNode Inspect()
        {
            return new QueryInspectionNode($"{GetType().Name}",
                            parameters: new Dictionary<string, string>()
                            {
                                { "Field", _fieldName.ToString() },
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
        private readonly Slice _fieldName;
        private readonly TVal _low, _high;
        private readonly CompactTree _tree;
        private readonly FixedSizeTree<TVal> _set;
        private readonly FixedSizeTree<TVal>.IFixedSizeIterator _iterator;
        private bool _first;

        private const int TermBufferSize = 32;
        private fixed byte _termsBuffer[TermBufferSize];
        private ByteStringContext<ByteStringMemoryCache>.ExternalScope _prevTermScope;

        public TermNumericRangeProvider(IndexSearcher searcher, FixedSizeTree<TVal> set,
            CompactTree tree, Slice fieldName, TVal low, TVal high)
        {
            _searcher = searcher;
            _fieldName = fieldName;
            _iterator = set.Iterate();
            _low = low;
            _high = high;
            _set = set;
            _tree = tree;
            _prevTermScope = default;
            _first = true;
        }

        public void Reset()
        {
            _first = true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Next(out TermMatch term) => Next(out term, out _);

        public bool Next(out TermMatch term, out Slice termSlice)
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
                    return Next(out term, out termSlice);
                }
            }     


            var cmp = _high.CompareTo(curVal);
            if (typeof(THigh) == typeof(Range.Exclusive) && cmp <= 0 || 
                typeof(THigh) == typeof(Range.Inclusive) && cmp < 0)
            {
                goto Empty;
            }

            _prevTermScope.Dispose();
            
            fixed (byte* p = _termsBuffer)
            {
                int termLen;
                if (typeof(TVal) == typeof(long))
                {
                    if (Utf8Formatter.TryFormat((long)(object)curVal, new Span<byte>(p, TermBufferSize), out termLen) == false)
                    {
                        throw new InvalidOperationException("Cannot format long value " + curVal);
                    }
                }
                else if(typeof(TVal) == typeof(double))
                {
                    if (Utf8Formatter.TryFormat((double)(object)curVal, new Span<byte>(p, TermBufferSize), out termLen) == false)
                    {
                        throw new InvalidOperationException("Cannot format double value " + curVal);
                    }
                }
                else
                {
                    throw new NotSupportedException("Unknown type: " + typeof(TVal));
                }

                _prevTermScope = Slice.External(_set.Llt.Allocator, p, termLen, ByteStringType.Immutable, out termSlice);

                term = _searcher.TermQuery(_tree, termSlice);
                return true;
            }

            Empty:
            termSlice = Slices.Empty;
            term = TermMatch.CreateEmpty();
            return false;
        }

        public QueryInspectionNode Inspect()
        {
            return new QueryInspectionNode($"{GetType().Name}",
                            parameters: new Dictionary<string, string>()
                            {
                                { "Field", _fieldName.ToString() },
                                { "Low", _low.ToString()},
                                { "High", _high.ToString()}
                            });
        }

        public string DebugView => Inspect().ToString();
    }
}
