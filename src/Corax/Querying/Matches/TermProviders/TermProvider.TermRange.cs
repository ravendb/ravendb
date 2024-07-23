using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using Corax.Indexing;
using Corax.Mappings;
using Corax.Querying.Matches.Meta;
using Corax.Utils;
using Sparrow;
using Sparrow.Compression;
using Sparrow.Server;
using Voron;
using Voron.Data.CompactTrees;
using Voron.Data.Containers;
using Voron.Data.Lookups;
using Voron.Data.PostingLists;
using Voron.Util;
using Range = Corax.Querying.Matches.Meta.Range;

namespace Corax.Querying.Matches.TermProviders;

[DebuggerDisplay("{DebugView,nq}")]
public struct TermRangeProvider<TLookupIterator, TLow, THigh> : ITermProvider, IAggregationProvider
    where TLookupIterator : struct, ILookupIterator
    where TLow : struct, Range.Marker
    where THigh : struct, Range.Marker
{
    private readonly IndexSearcher _indexSearcher;
    private readonly FieldMetadata _field;
    private Slice _low, _high;

    private CompactTree.Iterator<TLookupIterator> _iterator;

    private readonly bool _isForward;
    private bool _skipRangeCheck;
    private bool _isEmpty;
    private bool _shouldIncludeLastTerm;
    private long _endContainerId;

    public TermRangeProvider(Querying.IndexSearcher indexSearcher, CompactTree tree, in FieldMetadata field, Slice low, Slice high)
    {
        _indexSearcher = indexSearcher;
        _field = field;
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

    public bool IsFillSupported => true;

    public int Fill(Span<long> containers)
    {
        if (_isEmpty)
            return 0;

        return _iterator.Fill(containers, _endContainerId, _shouldIncludeLastTerm);
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

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Next(out TermMatch term)
    {
        if (_isEmpty || _iterator.MoveNext(out var termId) == false)
            goto ReturnEmpty;


        if (termId == _endContainerId)
        {
            _isEmpty = true;

            if (_shouldIncludeLastTerm == false)
                goto ReturnEmpty;
        }

        term = _indexSearcher.TermQuery(_field, termId, 1D);
        return true;

        ReturnEmpty:
        term = TermMatch.CreateEmpty(_indexSearcher, _indexSearcher.Allocator);
        return false;
    }

    public QueryInspectionNode Inspect()
    {
        var lowValue = _low.Options is SliceOptions.BeforeAllKeys
            ? null
            : _low.ToString();

        var highValue = _high.Options is SliceOptions.AfterAllKeys
            ? null
            : _high.ToString();

        return new QueryInspectionNode(nameof(TermRangeProvider<TLookupIterator, TLow, THigh>),
            parameters: new Dictionary<string, string>()
            {
                { Constants.QueryInspectionNode.FieldName, _field.ToString() },
                { Constants.QueryInspectionNode.LowValue, lowValue },
                { Constants.QueryInspectionNode.HighValue, highValue },
                { Constants.QueryInspectionNode.LowOption, typeof(TLow).Name },
                { Constants.QueryInspectionNode.HighOption, typeof(THigh).Name },
                { Constants.QueryInspectionNode.IteratorDirection, Constants.QueryInspectionNode.IterationDirectionName<TLookupIterator>() }
            });
    }

    public string DebugView => Inspect().ToString();

    public IDisposable AggregateByTerms(out List<string> terms, out Span<long> counts)
    {
        throw new NotImplementedException();
    }

    public unsafe long AggregateByRange()
    {
        //we do not support Long ranges since we want to perform aggregation on doubles 
        const long singleMarker = -1L;
        if (_isEmpty)
        {
            return 0;
        }
        
        var allocator = _indexSearcher.Allocator;
        CompactKey compactKey = new();
        compactKey.Initialize(_indexSearcher._transaction.LowLevelTransaction);
        
        NativeList<long> postingLists = new();
        postingLists.Initialize(allocator);
        
        NativeList<TermIdMask> postingListsType = new();
        postingListsType.Initialize(allocator);
        
        while (_isEmpty == false && _iterator.MoveNext(compactKey, out var termId, out var _))
        {
            if (termId == _endContainerId)
            {
                _isEmpty = true;

                if (_shouldIncludeLastTerm == false)
                    break;
            }
            
            if ((termId & (long)TermIdMask.PostingList) != 0)
            {
                postingLists.Add(allocator, EntryIdEncodings.GetContainerId(termId));
                postingListsType.Add(allocator, TermIdMask.PostingList);
            }
            else if ((termId & (long)TermIdMask.SmallPostingList) != 0)
            {
                postingLists.Add(allocator, EntryIdEncodings.GetContainerId(termId));
                postingListsType.Add(allocator, TermIdMask.SmallPostingList);
            }
            else
            {
                postingLists.Add(allocator, singleMarker);
                postingListsType.Add(allocator, TermIdMask.Single);
            }
        }

        using var _ = allocator.Allocate((sizeof(UnmanagedSpan)) * postingLists.Count, out ByteString containers);
        var containersPtr = (UnmanagedSpan*)containers.Ptr;

        Container.GetAll(_indexSearcher._transaction.LowLevelTransaction, postingLists.ToSpan(), containersPtr, singleMarker,
            _indexSearcher._transaction.LowLevelTransaction.PageLocator);

        long totalCount = 0;
        for (int i = 0; i < postingLists.Count; ++i)
        {
            var localCount = (postingListsType[i]) switch
            {
                TermIdMask.PostingList => ((PostingListState*)(containersPtr[i].Address))->NumberOfEntries,
                TermIdMask.SmallPostingList => VariableSizeEncoding.Read<long>(containersPtr[i].Address, out var _),
                TermIdMask.Single => 1,
                _ => throw new InvalidDataException($"Supported posting lists types are: 'PostingList', 'SmallPostingList', 'Single' but got {postingListsType[i]}")
            };
            
            totalCount += localCount;
        }

        postingLists.Dispose(allocator);
        postingListsType.Dispose(allocator);
        return totalCount;
    }
}
