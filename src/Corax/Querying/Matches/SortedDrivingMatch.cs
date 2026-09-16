using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Corax.Indexing;
using Corax.Mappings;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Primitives;
using Corax.Utils;
using Sparrow;
using Sparrow.Compression;
using Sparrow.Server;
using Voron;
using Voron.Data.Containers;
using Voron.Data.PostingLists;
using Voron.Data.RoaringBitmaps;
using Voron.Impl;
using Voron.Util;
using Voron.Util.PFor;

namespace Corax.Querying.Matches;

/// <summary>
/// Walks an ITermsProvider in term order, decoding each posting list and yielding
/// entry IDs directly. Unlike TermsProviderMatch (which materializes into a RoaringBitmap,
/// losing sort order), this yields entries grouped by term — preserving field-value order.
///
/// The ITermsProvider enforces range bounds (TermsRangeProvider only yields terms in range), so a WHERE
/// on the sort field both filters and sorts in one pass with no bitmap intermediate. Single-field
/// ORDER BY, e.g.:
///   FROM Users ORDER BY LastName
///   FROM Orders WHERE CreatedAt &gt; '2024-01-01' ORDER BY CreatedAt
///
/// Applies no WHERE predicates itself. Residual predicates on other fields are run by the wrapping
/// <see cref="DirectScanMatch"/> against each entry's stored fields. The planner gates on selectivity:
/// efficient when the driving field is selective, degrades when the residual rejects most entries.
/// </summary>
public sealed unsafe class SortedDrivingMatch : IQueryMatch, IDisposable
{
    private readonly ITermsProvider _provider;
    private readonly LowLevelTransaction _llt;
    private readonly ByteStringContext _allocator;

    // Dedup for multi-value fields
    private RoaringBitmap _emittedBitmap;

    // State for resuming across Fill calls
    private bool _providerExhausted;

    // Pending entries from a partially-consumed posting list
    private PostingList _pendingPostingList;
    private PostingList.Iterator _pendingLargeIterator;
    private bool _hasPendingLargeIterator;
    private FastPForBufferedReader _smallListReader;
    private bool _hasSmallListReader;

    private NativeList<long> _plIdsBuffer;
    private NativeList<UnmanagedSpan> _smallContainerItems;
    private int _plIdsRead;
    private int _plIdsIdx;
    private int _smallItemsIdx;

    private readonly bool _nullFirst;
    private readonly PostingList _nullPostingList;
    private readonly PostingList.Iterator _nullIterator;
    private readonly bool _hasNullPostingList;
    private bool _nullExhausted;

    // Non-existing entries (docs where the sort field was absent) are treated as null-adjacent:
    //   nullFirst=true → non-existing, then nulls, then normal values
    //   nullFirst=false → normal values, then nulls, then non-existing
    private readonly PostingList _nonExistingPostingList;
    private readonly PostingList.Iterator _nonExistingIterator;
    private readonly bool _hasNonExistingPostingList;
    private bool _nonExistingExhausted;

    public SortedDrivingMatch(ITermsProvider provider, LowLevelTransaction llt, ByteStringContext allocator,
        IndexSearcher searcher, FieldMetadata field, bool nullFirst)
    {
        _provider = provider;
        _llt = llt;
        _allocator = allocator;
        _nullFirst = nullFirst;
        _emittedBitmap = new RoaringBitmap(allocator);

        _hasNullPostingList = searcher.TryGetPostingListForNull(in field, out var nullPostingListId);
        _nullExhausted = !_hasNullPostingList;
        if (_hasNullPostingList)
            InitPostingList(out _nullPostingList, out _nullIterator, nullPostingListId);

        _hasNonExistingPostingList = searcher.TryGetPostingListForNonExisting(in field, out var nonExistingPostingListId);
        _nonExistingExhausted = !_hasNonExistingPostingList;
        if (_hasNonExistingPostingList)
            InitPostingList(out _nonExistingPostingList, out _nonExistingIterator, nonExistingPostingListId);

    }

    /// <summary>
    /// Compound-driven variant — serves a query like
    /// <c>FROM Orders WHERE Company = 'companies/1' ORDER BY OrderedAt</c> on a compound (Company, OrderedAt) index.
    /// The provider walks the compound subtree for the pinned first field, so it already yields the second field in
    /// order within that prefix.
    /// 
    /// The planner rejects scans when the field has null / missing values and the null sort mode isn't a match to the
    /// physically sorted layout and falls back to bitmap + SortingMatch.
    /// </summary>
    public SortedDrivingMatch(ITermsProvider provider, LowLevelTransaction llt, ByteStringContext allocator)
    {
        _provider = provider;
        _llt = llt;
        _allocator = allocator;
        _nullFirst = false;
        _emittedBitmap = new RoaringBitmap(allocator);

        // No global null / non-existing merge for the compound-scoped scan (see summary).
        _hasNullPostingList = false;
        _nullExhausted = true;
        _hasNonExistingPostingList = false;
        _nonExistingExhausted = true;
    }

    public long Count => -1;
    public bool IsBoosting => false;

    [SkipLocalsInit]
    public int Fill(Span<long> matches)
    {
        Span<long> entryBuffer = stackalloc long[QueryPrimitives.EntryScanBatchSize];
        if (_nullFirst && (_nonExistingExhausted && _nullExhausted) is false)
        {
            // If nulls-first, drain non-existing and null iterators at the start of every Fill call.
            HandleNullOrNonExistent();
        }
        else if (_nullFirst is false && _providerExhausted)
        {
            // After the provider is exhausted, drain nulls/non-existing if they appear last
            HandleNullOrNonExistent();
        }

        int count = 0;
        if (_hasPendingLargeIterator)
        {
            count += DrainLargePostingList(matches[count..], entryBuffer);
            if (count >= matches.Length)
                return count;
        }

        if (_hasSmallListReader)
        {
            count += DrainSmallPostingList(matches[count..], entryBuffer);
            if (count >= matches.Length)
                return count;
        }

        if (_plIdsBuffer.IsValid == false)
            _plIdsBuffer.Initialize(_allocator, QueryPrimitives.EntryScanBatchSize);
        if (_smallContainerItems.IsValid == false)
            _smallContainerItems.Initialize(_allocator, QueryPrimitives.EntryScanBatchSize);
        var pageLocator = _llt.PageLocator;

        while (count < matches.Length)
        {
            if (_plIdsIdx >= _plIdsRead)
            {
                _plIdsRead = _provider.FillPostingListIds(new Span<long>(_plIdsBuffer.RawItems, _plIdsBuffer.Capacity));
                if (_plIdsRead == 0)
                {
                    _providerExhausted = true;
                    break;
                }
                _plIdsIdx = 0;
                _smallItemsIdx = 0;

                // Batch-resolve SmallPostingList container items for this batch.
                int smallCount = 0;
                for (int i = 0; i < _plIdsRead; i++)
                {
                    long plId = _plIdsBuffer.RawItems[i];
                    var termType = (TermIdMask)plId & TermIdMask.EnsureIsSingleMask;
                    if (termType == TermIdMask.SmallPostingList)
                        entryBuffer[smallCount++] = (long)EntryIdEncodings.GetContainerId(plId);
                }
                if (smallCount > 0)
                {
                    Container.GetAll(_llt, entryBuffer[..smallCount],
                        new Span<UnmanagedSpan>(_smallContainerItems.RawItems, smallCount), pageLocator);
                }
            }

            while (_plIdsIdx < _plIdsRead && count < matches.Length)
            {
                long plId = _plIdsBuffer.RawItems[_plIdsIdx];
                var termType = (TermIdMask)plId & TermIdMask.EnsureIsSingleMask;

                if (termType == TermIdMask.Single)
                {
                    // Accumulate a run of consecutive Single-term entries (in sort/emit order) into the output, avoid linear Contains() calls
                    int runStart = count;
                    int runLimit = Math.Min(matches.Length, count + QueryPrimitives.EntryScanBatchSize);
                    while (_plIdsIdx < _plIdsRead && count < runLimit)
                    {
                        long runPlId = _plIdsBuffer.RawItems[_plIdsIdx];
                        if (((TermIdMask)runPlId & TermIdMask.EnsureIsSingleMask) != TermIdMask.Single)
                            break;
                        matches[count++] = (long)EntryIdEncodings.GetContainerId(runPlId);
                        _plIdsIdx++;
                    }
                    count = runStart + _emittedBitmap.DedupAddNew(matches.Slice(runStart, count - runStart), count - runStart);
                    continue;
                }

                switch (termType)
                {
                    case TermIdMask.SmallPostingList:
                    {
                        var item = _smallContainerItems.RawItems[_smallItemsIdx++];
                        _ = VariableSizeEncoding.Read<int>(item.Address, out var offset);
                        if (_smallListReader.WasInitialized == false)
                            _smallListReader = new FastPForBufferedReader(_llt.Allocator);
                        _smallListReader.Init(item.Address + offset, item.Length - offset);
                        _hasSmallListReader = true;
                        _plIdsIdx++;
                        count += DrainSmallPostingList(matches[count..], entryBuffer);
                        break;
                    }
                    case TermIdMask.PostingList:
                    {
                        var setStateSpan = Container.GetReadOnly(_llt, EntryIdEncodings.GetContainerId(plId));
                        ref readonly var setState = ref MemoryMarshal.AsRef<PostingListState>(setStateSpan);
                        _pendingPostingList = new PostingList(_llt, Slices.Empty, in setState);
                        _pendingLargeIterator = _pendingPostingList.Iterate();
                        _hasPendingLargeIterator = true;
                        _plIdsIdx++;
                        count += DrainLargePostingList(matches[count..], entryBuffer);
                        break;
                    }
                    default:
                        _plIdsIdx++;
                        break;
                }
            }
        }

        if (_nullFirst is false && count is 0 && _providerExhausted && (_nonExistingExhausted && _nullExhausted) is false)
        {
            // we have now exhausted the provider, but have no entries (all fields are null?), we still need
            // to return the null values, easiest is to recurse to fetch them
            RuntimeHelpers.EnsureSufficientExecutionStack();
            return Fill(matches);
        }

        return count;
    }

    private void HandleNullOrNonExistent()
    {
        // A null/non-existing posting list can span multiple Fill batches, let it finish first.
        if (_hasPendingLargeIterator)
            return;

        // Drain order must follow the documented field ordering (see the fields above):
        //   nullFirst == true  -> non-existing, then null
        //   nullFirst == false -> null, then non-existing
        if (_nullFirst)
        {
            if (QueueNonExisting() == false)
                QueueNull();
        }
        else
        {
            if (QueueNull() == false)
                QueueNonExisting();
        }

        bool QueueNonExisting()
        {
            if (_nonExistingExhausted is false && _hasNonExistingPostingList)
            {
                _pendingLargeIterator = _nonExistingIterator;
                _hasPendingLargeIterator = true;
                _nonExistingExhausted = true;
                return true;
            }
            return false;
        }

        bool QueueNull()
        {
            if (_nullExhausted is false && _hasNullPostingList)
            {
                _pendingLargeIterator = _nullIterator;
                _hasPendingLargeIterator = true;
                _nullExhausted = true;
                return true;
            }
            return false;
        }
    }

    private int DrainSmallPostingList(Span<long> matches, Span<long> entryBuffer)
    {
        int count = 0;
        fixed (long* pBuffer = entryBuffer)
        {
            while (count < matches.Length)
            {
                int slotsLeft = matches.Length - count;
                int requestSize = Math.Min(entryBuffer.Length, slotsLeft);
                int read = _smallListReader.Fill(pBuffer, requestSize);
                if (read <= 0)
                {
                    _hasSmallListReader = false;
                    break;
                }
                EntryIdEncodings.DecodeAndDiscardFrequency(entryBuffer, read);
                int newCount = _emittedBitmap.DedupAddNew(entryBuffer, read);
                entryBuffer[..newCount].CopyTo(matches[count..]);
                count += newCount;
            }
        }
        return count;
    }

    private int DrainLargePostingList(Span<long> matches, Span<long> entryBuffer)
    {
        int count = 0;
        while (count < matches.Length)
        {
            int slotsLeft = matches.Length - count;
            int requestSize = Math.Min(entryBuffer.Length, slotsLeft);
            var request = entryBuffer[..requestSize];
            if (_pendingLargeIterator.Fill(request, out int read) == false || read == 0)
            {
                // Iterator exhausted: clear the pending flag so later Fills move on to the next term
                _hasPendingLargeIterator = false;
                _pendingPostingList?.Dispose();
                _pendingPostingList = null;
                break;
            }
            EntryIdEncodings.DecodeAndDiscardFrequency(request, read);
            int newCount = _emittedBitmap.DedupAddNew(request, read);
            request[..newCount].CopyTo(matches[count..]);
            count += newCount;
        }
        return count;
    }

    private void InitPostingList(out PostingList postingList, out PostingList.Iterator iterator, long postingListId)
    {
        var containerEntryId = EntryIdEncodings.GetContainerId(postingListId);
        var setStateSpan = Container.GetReadOnly(_llt, containerEntryId);
        ref readonly var setState = ref MemoryMarshal.AsRef<PostingListState>(setStateSpan);
        postingList = new PostingList(_llt, Slices.Empty, in setState);
        iterator = postingList.Iterate();
    }

    public void Score(Span<long> matches, Span<float> scores, float boostFactor) { }
    public void ScoreSorted(Span<long> matches, Span<float> scores, float boostFactor) { }

    public QueryInspectionNode Inspect()
    {
        return new QueryInspectionNode("SortedDrivingMatch",
            parameters: new Dictionary<string, string>
            {
                ["Provider"] = _provider.Inspect().Operation
            });
    }

    public void Dispose()
    {
        if (_smallListReader.WasInitialized)
            _smallListReader.Dispose();
        _pendingPostingList?.Dispose();
        _nullPostingList?.Dispose();
        _nonExistingPostingList?.Dispose();
        _emittedBitmap.Dispose();
        _plIdsBuffer.Dispose(_allocator);
        _smallContainerItems.Dispose(_allocator);
    }
}
