using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using Sparrow.Server;
using Voron;
using Voron.Data.CompactTrees;

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
}
