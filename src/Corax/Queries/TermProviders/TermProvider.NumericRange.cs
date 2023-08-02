using System.Collections.Generic;
using System.Diagnostics;
using Corax.Mappings;
using Corax.Queries.Meta;
using Sparrow.Extensions;
using Voron.Data.Lookups;


namespace Corax.Queries.TermProviders
{
    [DebuggerDisplay("{DebugView,nq}")]
    public struct TermNumericRangeProvider<TLookupIterator, TLow, THigh, TVal> : ITermProvider
        where TLookupIterator : struct, ILookupIterator
        where TLow : struct, Range.Marker
        where THigh  : struct, Range.Marker
        where TVal : struct, ILookupKey
    {
        private readonly IndexSearcher _searcher;
        private readonly FieldMetadata _field;
        private readonly TVal _low, _high;
        private TLookupIterator _iterator;
        private bool _first;
        private readonly bool _skipRangeCheck;

        public TermNumericRangeProvider(IndexSearcher searcher, Lookup<TVal> set, FieldMetadata field, TVal low, TVal high)
        {
            _searcher = searcher;
            _field = field;
            _iterator = set.Iterate<TLookupIterator>();
            _low = low;
            _high = high;
            _first = true;

            //Unbounded query can skip checking range after first element (since we're taking ALL possible results from starting point)
            _skipRangeCheck = _iterator.IsForward switch
            {
                true when typeof(THigh) == typeof(Range.Inclusive) && typeof(TVal) == typeof(Int64LookupKey) && ((Int64LookupKey)(object)high).Value == long.MaxValue => true,
                true when typeof(THigh) == typeof(Range.Inclusive) && typeof(TVal) == typeof(DoubleLookupKey) && ((DoubleLookupKey)(object)high).Value.AlmostEquals(double.MaxValue) => true,
                false when typeof(TLow) == typeof(Range.Inclusive) && typeof(TVal) == typeof(Int64LookupKey) && ((Int64LookupKey)(object)low).Value == long.MinValue => true,
                false when typeof(TLow) == typeof(Range.Inclusive) && typeof(TVal) == typeof(DoubleLookupKey) && ((DoubleLookupKey)(object)low).Value.AlmostEquals(double.MinValue) => true,
                _ => false
            };
        }
        
        public void Reset()
        {
            _first = true;
        }
        
        public bool Next(out TermMatch term)
        {
            bool wasFirst = false;
            if (_first)
            {
                _iterator.Seek(_iterator.IsForward ? _low : _high);
                _first = false;
                wasFirst = true;
            }

            bool hasNext = _iterator.MoveNext(out TVal key, out var termId);
            if (hasNext == false)
                goto Empty;


            if (wasFirst)
            {
                switch (_iterator.IsForward)
                {
                    case true when typeof(TLow) == typeof(Range.Exclusive) && key.IsEqual(_low):
                        return Next(out term);
                    case false when typeof(THigh) == typeof(Range.Exclusive) && _high.CompareTo(key) <= 0:
                        return Next(out term);
                    case false when typeof(THigh) == typeof(Range.Inclusive) && _high.CompareTo(key) < 0:
                        return Next(out term);
                }
            }
            
            //In case of going forward we've to compare with the highest (right element). In case of backward we've to compare to the lowest element.
            if (_skipRangeCheck == false)
            {
                if (_iterator.IsForward)
                {
                    var cmp = _high.CompareTo(key);
                    if (typeof(THigh) == typeof(Range.Exclusive) && cmp <= 0 ||
                        typeof(THigh) == typeof(Range.Inclusive) && cmp < 0)
                    {
                        goto Empty;
                    }
                }
                else
                {
                    var cmp = _low.CompareTo(key);
                    if (typeof(TLow) == typeof(Range.Exclusive) && cmp >= 0 ||
                        typeof(TLow) == typeof(Range.Inclusive) && cmp > 0)
                        goto Empty;
                }
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
