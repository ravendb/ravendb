using System;
using System.Collections.Generic;
using System.Diagnostics;
using Corax.Indexing;
using Corax.Mappings;
using Corax.Querying.Matches.Meta;
using Voron;
using Voron.Data.CompactTrees;
using Voron.Data.Lookups;
using Voron.Util;
using Range = Corax.Querying.Matches.Meta.Range;

namespace Corax.Querying.Matches.TermsProviders;

[DebuggerDisplay("{DebugView,nq}")]
public struct TermsRangeProvider<TLookupIterator, TLow, THigh> : ITermsProvider, IAggregationProvider
    where TLookupIterator : struct, ILookupIterator
    where TLow : struct, Range.Marker
    where THigh : struct, Range.Marker
{
    private readonly IndexSearcher _indexSearcher;
    private readonly FieldMetadata _field;
    private readonly CompactTree _tree;
    private Slice _low, _high;

    private CompactTree.Iterator<TLookupIterator> _iterator;

    private readonly bool _isForward;
    private bool _skipRangeCheck;
    private bool _isEmpty;
    private bool _shouldIncludeLastTerm;
    private long _endContainerId;

    public TermsRangeProvider(IndexSearcher indexSearcher, CompactTree tree, in FieldMetadata field, Slice low, Slice high)
    {
        _indexSearcher = indexSearcher;
        _field = field;
        _tree = tree;
        _iterator = tree.Iterate<TLookupIterator>();
        _isForward = default(TLookupIterator).IsForward;


        _low = low;
        _high = high;

        // Optimization for unbounded ranges. We seek the proper term (depending on the iterator) and iterate through all left items.
        _skipRangeCheck = _isForward
            ? _high.Options is SliceOptions.AfterAllKeys
            : _low.Options is SliceOptions.BeforeAllKeys;
        PrepareKeys();
        Reset();
    }


    private void PrepareKeys()
    {
        CompactKey key;
        ReadOnlySpan<byte> termSlice;

        var startKey = _isForward ? _low : _high;
        var finalKey = _isForward ? _high : _low;

        if (ShouldSeek())
        {
            _iterator.Seek(startKey);
            if (_iterator.MoveNext(out key, out _, out _) == false)
            {
                _isEmpty = true;
                return; //empty set, we will go out of range immediately 
            }

            termSlice = key.Decoded();
            var shouldInclude = _isForward switch
            {
                false when typeof(THigh) == typeof(Range.Exclusive) && termSlice.SequenceCompareTo(_high.AsSpan()) >= 0 => false,
                false when typeof(THigh) == typeof(Range.Inclusive) && _high.Options != SliceOptions.AfterAllKeys &&
                           termSlice.SequenceCompareTo(_high.AsSpan()) > 0 => false,
                true when typeof(TLow) == typeof(Range.Exclusive) && termSlice.SequenceCompareTo(_low.AsSpan()) <= 0 => false,
                true when typeof(TLow) == typeof(Range.Inclusive) && _low.Options != SliceOptions.BeforeAllKeys &&
                          termSlice.SequenceCompareTo(_low.AsSpan()) < 0 => false,
                _ => true
            };

            if (shouldInclude == false)
            {
                if (_iterator.MoveNext(out key, out _, out _) == false)
                {
                    _isEmpty = true;
                    return; //empty set, we will go out of range immediately
                }

                termSlice = key.Decoded();

                //Next seek will go immediately to the right term.
                if (_isForward)
                    Slice.From(_indexSearcher.Allocator, termSlice, out _low);
                else
                    Slice.From(_indexSearcher.Allocator, termSlice, out _high);
            }
        }

        if (_skipRangeCheck)
        {
            // In this case we will accept all items left.
            _endContainerId = long.MaxValue;
            _shouldIncludeLastTerm = true;
            return;
        }


        _iterator.Seek(finalKey);
        if (_iterator.MoveNext(out key, out _endContainerId, out var hasPreviousValue) == false)
        {
            _skipRangeCheck = true; //we are out of item anyway that means we can accept all items
            _endContainerId = long.MaxValue;
            return;
        }

        termSlice = key.Decoded();
        var finalCmp = termSlice.SequenceCompareTo(finalKey.AsSpan());

        _shouldIncludeLastTerm = _isForward switch
        {
            false when typeof(TLow) == typeof(Range.Exclusive) && finalCmp <= 0 => false,
            false when typeof(TLow) == typeof(Range.Inclusive) && finalCmp < 0 => false,
            true when typeof(THigh) == typeof(Range.Exclusive) && finalCmp >= 0 => false,
            true when typeof(THigh) == typeof(Range.Inclusive) && _high.Options != SliceOptions.AfterAllKeys && finalCmp > 0 => false,
            _ => true
        };
        if (_shouldIncludeLastTerm == false && hasPreviousValue == false)
        {
            _isEmpty = true;
        }
    }

    public int FillPostingListIds(Span<long> postingListIds)
    {
        if (_isEmpty)
            return 0;

        return _iterator.Fill(postingListIds, _endContainerId, _shouldIncludeLastTerm);
    }

    public void Reset()
    {
        var shouldSeek = ShouldSeek();
        if (shouldSeek)
            _iterator.Seek(_isForward ? _low : _high);
        else
            _iterator.Reset();
    }

    private bool ShouldSeek()
    {
        return _isForward switch
        {
            true when _low.Options != SliceOptions.BeforeAllKeys => true,
            false when _high.Options != SliceOptions.AfterAllKeys => true,
            _ => false
        };
    }

    public QueryInspectionNode Inspect()
    {
        var lowValue = _low.Options is SliceOptions.BeforeAllKeys
            ? null
            : _low.ToString();

        var highValue = _high.Options is SliceOptions.AfterAllKeys
            ? null
            : _high.ToString();

        return new QueryInspectionNode(nameof(TermsRangeProvider<,,>),
            parameters: new Dictionary<string, string>()
            {
                { Constants.QueryInspectionNode.FieldName, _field.FieldName.ToString() },
                { Constants.QueryInspectionNode.LowValue, lowValue },
                { Constants.QueryInspectionNode.HighValue, highValue },
                { Constants.QueryInspectionNode.LowOption, typeof(TLow).Name },
                { Constants.QueryInspectionNode.HighOption, typeof(THigh).Name },
                { Constants.QueryInspectionNode.IteratorDirection, Constants.QueryInspectionNode.IterationDirectionName<TLookupIterator>() }
            });
    }

    public string DebugView => Inspect().ToString();

    public IDisposable AggregateByTerms(out List<string> terms, out Span<long> counts) => throw new NotImplementedException();

    public long AggregateByRange()
    {
        //we do not support Long ranges since we want to perform aggregation on doubles
        if (_isEmpty)
            return 0;

        // maxTerms: 0 -> scan every in-range term, giving the exact (multi-valued-overcounting) total.
        return CountPostingsInRange(maxTerms: 0).Postings;
    }

    /// <summary>
    /// Header-only walk over the in-range terms (capped at <paramref name="maxTerms"/>; 0 = all), partitioning them
    /// into the per-type buckets and reading their headers via <see cref="RangePostingBuckets"/>. The returned
    /// breakdown (total postings plus the single / small / large split and their sub-totals) is the raw material the
    /// two-ended range-cardinality probe extrapolates from.
    /// </summary>
    public unsafe RangePostingStats CountPostingsInRange(int maxTerms)
    {
        var stats = new RangePostingStats();
        if (_isEmpty)
            return stats;

        var allocator = _indexSearcher.Allocator;
        var llt = _indexSearcher._transaction.LowLevelTransaction;
        CompactKey compactKey = llt.AcquireCompactKey();

        Span<NativeList<long>> buckets = stackalloc NativeList<long>[RangePostingBuckets.Count];
        RangePostingBuckets.Initialize(buckets, allocator);

        try
        {
            while (_isEmpty == false && _iterator.MoveNext(compactKey, out var termId))
            {
                if (termId == _endContainerId)
                {
                    _isEmpty = true;

                    if (_shouldIncludeLastTerm == false)
                        break;
                }

                buckets[(int)(termId & (long)TermIdMask.EnsureIsSingleMask)].Add(allocator, termId);
                stats.Terms++;

                if (maxTerms > 0 && stats.Terms >= maxTerms)
                    break;
            }

            RangePostingBuckets.Summarize(buckets, allocator, llt, ref stats);
            return stats;
        }
        finally
        {
            RangePostingBuckets.Release(buckets, allocator);
            llt.ReleaseCompactKey(ref compactKey);
        }
    }

    /// <summary>
    /// Sub-linear estimate of how many distinct terms fall in this provider's range, forwarding to
    /// <see cref="CompactTree.GetNumberOfEntriesInRangeEstimate"/>. Open bounds are estimated directly: a
    /// "before all keys" low is the empty span (sorts before every term, descending the leftmost leaf) and an
    /// "after all keys" high descends the rightmost leaf, so an open-ended range counts to the edge of the tree.
    /// </summary>
    public long EstimateTermCountInRange()
    {
        if (_isEmpty)
            return 0;

        // A "before all keys" low bound is represented by the empty span, which sorts before every stored key.
        var lowSpan = _low.Options == SliceOptions.BeforeAllKeys ? ReadOnlySpan<byte>.Empty : _low.AsSpan();
        var highSpan = _high.Options == SliceOptions.AfterAllKeys ? ReadOnlySpan<byte>.Empty : _high.AsSpan();
        // An "after all keys" high has no concrete key to seek; signal the descent to walk to the rightmost leaf.
        return _tree.GetNumberOfEntriesInRangeEstimate(lowSpan, highSpan);
    }

    /// <summary>Total number of terms stored for this field (O(1)); used by the cardinality combiner's whale guard.</summary>
    public long TotalTermCount() => _tree.NumberOfEntries;
}
