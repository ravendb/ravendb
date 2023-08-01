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

    private readonly bool _skipHighCheck;
    private bool _skipFirstCheck;
    private readonly bool isForward;
    private bool _isFirst;

    public TermRangeProvider(IndexSearcher searcher, CompactTree tree, FieldMetadata field, Slice low, Slice high)
    {
        _searcher = searcher;
        _field = field;
        _iterator = tree.Iterate<TLookupIterator>();
        isForward = default(TLookupIterator).IsForward;
        _low = low;
        _high = high;
        _tree = tree;
        _skipFirstCheck = false;
        _skipHighCheck = high.Options == SliceOptions.AfterAllKeys;
        Reset();
    }

    public void Reset()
    {
        _isFirst = true;
        if (isForward)
        {
            if (_low.Options != SliceOptions.BeforeAllKeys)
            {
                _iterator.Seek(_low);
                _skipFirstCheck = typeof(TLow) == typeof(Range.Exclusive);
            }
            else
            {
                _iterator.Reset();
            }
        }
        else
        {
            _skipFirstCheck = typeof(THigh) == typeof(Range.Exclusive);
            _iterator.Seek(_high);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Next(out TermMatch term) => Next(out term, out _);

    public bool Next(out TermMatch term, out CompactKey key)
    {
        bool wasFirst = false;
            
        if (_iterator.MoveNext(out key, out var _) == false)
        {
            term = TermMatch.CreateEmpty(_searcher, _searcher.Allocator);
            return false;
        }

        var termSlice = key.Decoded();

        if (_isFirst)
        {
            wasFirst = true;
            _isFirst = false;

            //Since we seeked just one item before
            if (isForward == false)
            {
                    
            }
        }
            
            
        if (typeof(TLow) == typeof(Range.Exclusive))
        {
            if (_skipFirstCheck)
            {
                _skipFirstCheck = false;
                if (_low.AsSpan().SequenceEqual(termSlice))
                {
                    return Next(out term, out key);
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
            
        term = _searcher.TermQuery(_field, key, _tree);
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
