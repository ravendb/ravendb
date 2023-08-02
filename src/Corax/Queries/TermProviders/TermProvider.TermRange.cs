using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Corax.Mappings;
using Voron;
using Voron.Data.CompactTrees;
using Voron.Data.Lookups;
using Range = Corax.Queries.Meta.Range;

namespace Corax.Queries.TermProviders;

[DebuggerDisplay("{DebugView,nq}")]
public struct TermRangeProvider<TLookupIterator, TLow, THigh> : ITermProvider
    where TLookupIterator : struct, ILookupIterator
    where TLow : struct, Range.Marker
    where THigh  : struct, Range.Marker
{
    private readonly CompactTree _tree;
    private readonly IndexSearcher _searcher;
    private readonly FieldMetadata _field;
    private readonly Slice _low, _high;

    private CompactTree.Iterator<TLookupIterator> _iterator;

    private readonly bool _isForward;
    private readonly bool _skipRangeCheck;
    private bool _isFirst;

    public TermRangeProvider(IndexSearcher searcher, CompactTree tree, FieldMetadata field, Slice low, Slice high)
    {
        _searcher = searcher;
        _field = field;
        _iterator = tree.Iterate<TLookupIterator>();
        _isForward = default(TLookupIterator).IsForward;
        
        
        _low = low;
        _high = high;
        _tree = tree;

        // Optimization for unbounded ranges. We seek the proper term (depending on the iterator) and iterate through all left items.
        _skipRangeCheck = _isForward 
            ? _high.Options is SliceOptions.AfterAllKeys 
            : _low.Options is SliceOptions.BeforeAllKeys;
        
        Reset();
    }

    public void Reset()
    {
        _isFirst = true;

        //
        var shouldSeek = _isForward switch
        {
            true when _low.Options != SliceOptions.BeforeAllKeys => true,
            false when _high.Options != SliceOptions.AfterAllKeys => true,
            _ => false
        };

        
        if (shouldSeek)
            _iterator.Seek(_isForward ? _low : _high);
        else
            _iterator.Reset();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Next(out TermMatch term) => Next(out term, out _);

    private bool Next(out TermMatch term, out CompactKey key)
    {
        if (_iterator.MoveNext(out key, out var _) == false)
        {
            term = TermMatch.CreateEmpty(_searcher, _searcher.Allocator);
            return false;
        }
        

        if (_isFirst)
        {
            _isFirst = false;
            var termSlice = key.Decoded();

            switch (_isForward)
            {
                case false when typeof(THigh) == typeof(Range.Exclusive) && termSlice.SequenceCompareTo(_high.AsSpan()) >= 0:
                    return Next(out term);
                case false when typeof(THigh) == typeof(Range.Inclusive) && _high.Options != SliceOptions.AfterAllKeys && termSlice.SequenceCompareTo(_high.AsSpan()) > 0:
                    return Next(out term);
                case true when typeof(TLow) == typeof(Range.Exclusive) && termSlice.SequenceCompareTo(_low.AsSpan()) <= 0:
                    return Next(out term);
                case true when typeof(TLow) == typeof(Range.Inclusive) && _low.Options != SliceOptions.BeforeAllKeys && termSlice.SequenceCompareTo(_low.AsSpan()) < 0:
                    return Next(out term);
            }
        }
        
        if (_skipRangeCheck == false)
        {
            var termSlice = key.Decoded();
            if (_isForward)
            {
                var cmp = _high.AsSpan().SequenceCompareTo(termSlice);
                if (typeof(THigh) == typeof(Range.Exclusive) && cmp <= 0 ||
                    typeof(THigh) == typeof(Range.Inclusive) && cmp < 0)
                {
                    goto ReturnEmpty;
                }
            }
            else
            {
                var cmp = _low.AsSpan().SequenceCompareTo(termSlice);
                if (typeof(TLow) == typeof(Range.Exclusive) && cmp >= 0 ||
                    typeof(TLow) == typeof(Range.Inclusive) && cmp > 0)
                    goto ReturnEmpty;
            }
        }
            
        term = _searcher.TermQuery(_field, key, _tree);
        return true;
        
        ReturnEmpty:
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
