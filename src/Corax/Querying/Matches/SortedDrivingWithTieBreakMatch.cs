using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Corax.Indexing;
using Corax.Mappings;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Matches.SortingMatches;
using Corax.Querying.Matches.SortingMatches.Meta;
using Corax.Querying.Primitives;
using Corax.Utils;
using Sparrow;
using Sparrow.Compression;
using Sparrow.Server;
using Voron;
using Voron.Data.Containers;
using Voron.Data.Lookups;
using Voron.Data.PostingLists;
using Voron.Data.RoaringBitmaps;
using Voron.Impl;
using Voron.Util;
using Voron.Util.PFor;

namespace Corax.Querying.Matches;

/// <summary>
/// Two-field ORDER BY: like SortedDrivingMatch, but resolves ties within each primary term by a
/// secondary field (Integer, Floating, or Sequence). Walks the ITermsProvider in primary-term order;
/// per term, drains the posting list into a buffer, fetches secondary values via
/// Lookup&lt;Int64LookupKey&gt;.GetFor, sorts, and emits. E.g.:
///   FROM Orders ORDER BY Status, CreatedAt DESC
///   FROM Users WHERE Age &gt; 18 ORDER BY Age, LastName
///
/// To bound memory when many docs share one primary value, each primary-term group is capped at _maxGroupSize
/// and truncated to its top-<c>take</c> entries by secondary value (a bounded top-K heap). This is loss-free for
/// a paged query: only a group's top-<c>take</c> can ever reach the final result. An unbounded take (no LIMIT)
/// disables truncation (_maxGroupSize = int.MaxValue), so the group grows to hold every matching doc.
/// Same-field optimization as SortedDrivingMatch: a WHERE on the primary sort field narrows the
/// TermsRangeProvider. Residual predicates on other fields are evaluated by the wrapping
/// <see cref="DirectScanMatch"/>.
/// </summary>
public sealed unsafe class SortedDrivingWithTieBreakMatch : IQueryMatch, IDisposable
{
    private readonly ITermsProvider _provider;
    private readonly LowLevelTransaction _llt;
    private readonly ByteStringContext _allocator;
    private readonly Lookup<Int64LookupKey> _secondaryLookup;
    private readonly MatchCompareFieldType _secondaryType;
    private readonly bool _secondaryDescending;
    private readonly bool _nullIsSmallest;
    private readonly long _missingSecondaryValue;
    private readonly int _take;
    private readonly int _maxGroupSize;

    // String tie-break: container IDs for null/non-existing terms, resolved once in ctor.
    private readonly long _nullTermContainerId;
    private readonly long _nonExistingTermContainerId;

    private RoaringBitmap _emittedBitmap;
    private bool _providerExhausted;

    // Persistent primary plId batch
    private NativeList<long> _plIdsBuffer;
    private NativeList<UnmanagedSpan> _smallContainerItems;
    private int _plIdsRead;
    private int _plIdsIdx;
    private int _smallItemsIdx;

    // Per-term group state
    private NativeList<long> _groupEntries;
    private NativeList<long> _groupSecondary;
    private NativeList<int> _groupSortedIndexes;
    // String tie-break scratch: resolved CompactKey blobs per group entry.
    private NativeList<UnmanagedSpan> _groupTerms;
    // Parallel term buffers (sized to _take) for the bounded top-K heap in TruncateGroupToTopTake; only
    // one is allocated, picked by the secondary type. Unused when the take is unbounded (no truncation).
    private NativeList<long> _groupHeapTerms;
    private NativeList<UnmanagedSpan> _groupHeapTermsSeq;
    private int _groupEmitIdx;

    // no-value == null or non-existing
    //   nullFirst=true  → no-value group, then normal values
    //   nullFirst=false → normal values, then no-value group
    private readonly bool _nullFirst;
    private readonly long _nullPostingListId;
    private readonly bool _hasNullPostingList;
    private bool _nullExhausted;

    private readonly long _nonExistingPostingListId;
    private readonly bool _hasNonExistingPostingList;
    private bool _nonExistingExhausted;

    // Null/non-existing docs get the same per-group secondary sort as regular terms; tracks that load.
    private bool _nullGroupPrepared;

    public SortedDrivingWithTieBreakMatch(
        ITermsProvider provider,
        LowLevelTransaction llt,
        ByteStringContext allocator,
        IndexSearcher searcher,
        FieldMetadata primaryField,
        FieldMetadata secondaryField,
        MatchCompareFieldType secondaryType,
        bool secondaryDescending,
        bool nullFirst,
        bool nullIsSmallest,
        int take)
    {
        if (secondaryType is not (MatchCompareFieldType.Integer or MatchCompareFieldType.Floating or MatchCompareFieldType.Sequence))
            throw new NotSupportedException($"SortedDrivingWithTieBreakMatch only supports Integer, Floating, or Sequence tie-break fields (got {secondaryType})");

        _provider = provider;
        _llt = llt;
        _allocator = allocator;
        _nullFirst = nullFirst;
        _secondaryType = secondaryType;
        _secondaryDescending = secondaryDescending;
        _nullIsSmallest = nullIsSmallest;
        // When take is unbounded (TakeAll = -1) or very large, disable the group truncation
        // by setting _maxGroupSize to int.MaxValue — the group grows as needed without truncation.
        if (take is Constants.IndexSearcher.TakeAll or > int.MaxValue / 4)
        {
            _take = int.MaxValue;
            _maxGroupSize = int.MaxValue;
        }
        else
        {
            _take = Math.Max(take, 1);
            _maxGroupSize = Math.Max(
                RoaringBitmap.PadToVector256Width(_take * 4),
                QueryPrimitives.TieBreakGroupInitialCapacity);
        }
        _emittedBitmap = new RoaringBitmap(allocator);

        // Resolve the secondary Lookup using the type-specific field name (long/double suffix).
        Slice secondaryLookupName;
        switch (secondaryType)
        {
            case MatchCompareFieldType.Integer:
                IndexFieldsMappingBuilder.GetFieldNameForLongs(searcher.Allocator, secondaryField.FieldName, out secondaryLookupName);
                _missingSecondaryValue = nullIsSmallest ? long.MinValue : long.MaxValue;
                break;
            case MatchCompareFieldType.Floating:
                IndexFieldsMappingBuilder.GetFieldNameForDoubles(searcher.Allocator, secondaryField.FieldName, out secondaryLookupName);
                _missingSecondaryValue = BitConverter.DoubleToInt64Bits(nullIsSmallest ? double.MinValue : double.MaxValue);
                break;
            default:
                // Sequence: the lookup maps entry IDs to term container IDs (no type suffix).
                secondaryLookupName = secondaryField.FieldName;
                _missingSecondaryValue = SortingHelpers.MissingTermId;

                // Resolve null/non-existing term container IDs for the string path.
                if (searcher.TryGetPostingListForNull(secondaryField.FieldName, out _, out _nullTermContainerId) == false)
                    _nullTermContainerId = SortingHelpers.InvalidTermId;
                if (searcher.TryGetPostingListForNonExisting(secondaryField.FieldName, out _, out _nonExistingTermContainerId) == false)
                    _nonExistingTermContainerId = SortingHelpers.InvalidTermId;
                break;
        }
        _secondaryLookup = searcher.EntriesToTermsReader(secondaryLookupName);

        _hasNullPostingList = searcher.TryGetPostingListForNull(in primaryField, out _nullPostingListId);
        _nullExhausted = !_hasNullPostingList;

        _hasNonExistingPostingList = searcher.TryGetPostingListForNonExisting(in primaryField, out _nonExistingPostingListId);
        _nonExistingExhausted = !_hasNonExistingPostingList;

        // Allocate all persistent buffers up front — avoids 7 IsValid branches per Fill call.
        _plIdsBuffer.Initialize(allocator, QueryPrimitives.EntryScanBatchSize);
        _smallContainerItems.Initialize(allocator, QueryPrimitives.EntryScanBatchSize);
        _groupEntries.Initialize(allocator, QueryPrimitives.TieBreakGroupInitialCapacity);
        _groupSecondary.Initialize(allocator, QueryPrimitives.TieBreakGroupInitialCapacity);
        _groupSortedIndexes.Initialize(allocator, QueryPrimitives.TieBreakGroupInitialCapacity);
        if (secondaryType == MatchCompareFieldType.Sequence)
            _groupTerms.Initialize(allocator, QueryPrimitives.TieBreakGroupInitialCapacity);

        // Bounded-take path only: scratch term buffer the shared heap sorter keeps parallel to its
        // surviving indices (capacity _take). TakeAll never truncates, so it needs none.
        if (_maxGroupSize != int.MaxValue)
        {
            if (secondaryType == MatchCompareFieldType.Sequence)
                _groupHeapTermsSeq.Initialize(allocator, _take);
            else
                _groupHeapTerms.Initialize(allocator, _take);
        }
    }

    public long Count => -1;
    public bool IsBoosting => false;

    [SkipLocalsInit]
    public int Fill(Span<long> matches)
    {
        Span<long> entryBuffer = stackalloc long[QueryPrimitives.EntryScanBatchSize];
        int count = 0;

        // Nulls-first: load null-primary group and sort by secondary before regular terms.
        if (_nullFirst && _nullGroupPrepared == false)
            PrepareNullGroup(entryBuffer);

        // Emit any remaining entries from a previously-sorted group (null or regular).
        if (_groupEntries.Count > 0)
        {
            count += EmitFromSortedGroup(matches[count..]);
            if (count >= matches.Length)
                return count;
        }

        var pageLocator = _llt.PageLocator;

        while (count < matches.Length)
        {
            // Refill primary plIds batch from the provider when exhausted.
            if (_plIdsIdx >= _plIdsRead)
            {
                if (_providerExhausted)
                    break;
                _plIdsRead = _provider.FillPostingListIds(new Span<long>(_plIdsBuffer.RawItems, _plIdsBuffer.Capacity));
                if (_plIdsRead == 0)
                {
                    _providerExhausted = true;
                    break;
                }
                _plIdsIdx = 0;
                _smallItemsIdx = 0;

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

            // Drain the next primary term's posting list into _groupEntries, sort, then emit.
            while (_plIdsIdx < _plIdsRead && count < matches.Length)
            {
                long plId = _plIdsBuffer.RawItems[_plIdsIdx];
                var termType = (TermIdMask)plId & TermIdMask.EnsureIsSingleMask;

                if (termType == TermIdMask.Single)
                {
                    // A Single term is its own one-entry primary group, accumulate a run of consecutive Singles into the output
                    // and dedup the run in one bulk pass. No need for a secondary step
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

                _groupEntries.Clear();
                switch (termType)
                {
                    case TermIdMask.SmallPostingList:
                    {
                        var item = _smallContainerItems.RawItems[_smallItemsIdx++];
                        _ = VariableSizeEncoding.Read<int>(item.Address, out var offset);
                        var smallReader = new FastPForBufferedReader(_llt.Allocator);
                        try
                        {
                            smallReader.Init(item.Address + offset, item.Length - offset);
                            DrainSmallIntoGroup(ref smallReader, entryBuffer);
                        }
                        finally
                        {
                            if (smallReader.WasInitialized)
                                smallReader.Dispose();
                        }
                        _plIdsIdx++;
                        break;
                    }
                    case TermIdMask.PostingList:
                    {
                        var setStateSpan = Container.GetReadOnly(_llt, EntryIdEncodings.GetContainerId(plId));
                        ref readonly var setState = ref MemoryMarshal.AsRef<PostingListState>(setStateSpan);
                        using var pl = new PostingList(_llt, Slices.Empty, in setState);
                        var iter = pl.Iterate();
                        DrainLargeIntoGroup(ref iter, entryBuffer);
                        _plIdsIdx++;
                        break;
                    }
                    default:
                        throw new ArgumentException($"Unexpected TermIdMask value {termType} for plId {plId}");
                }

                if (_groupEntries.Count == 0)
                    continue;

                SortGroupBySecondary();
                count += EmitFromSortedGroup(matches[count..]);
                if (count >= matches.Length)
                    return count;
            }
        }

        // After the provider is exhausted, load and emit null-primary group (nulls-last).
        if (_providerExhausted && _nullFirst == false && _nullGroupPrepared == false)
        {
            PrepareNullGroup(entryBuffer);
            if (_groupEntries.Count> 0)
                count += EmitFromSortedGroup(matches[count..]);
        }

        return count;
    }

    private void EnsureGroupCapacity(int required)
    {
        int curCap = _groupEntries.Capacity;
        if (required <= curCap)
            return;

        int newCap = curCap;
        while (newCap < required)
            newCap = (int)Math.Min((long)newCap * 2, _maxGroupSize);
        int addition = newCap - curCap;

        _groupEntries.Grow(_allocator, addition);
        _groupSecondary.Grow(_allocator, addition);
        _groupSortedIndexes.Grow(_allocator, addition);
        if (_groupTerms.IsValid)
            _groupTerms.Grow(_allocator, addition);
    }

    private void DrainSmallIntoGroup(ref FastPForBufferedReader reader, Span<long> entryBuffer)
    {
        fixed (long* pBuffer = entryBuffer)
        {
            while (true)
            {
                int read = reader.Fill(pBuffer, entryBuffer.Length);
                if (read <= 0)
                    break;
                AddToGroup(entryBuffer, read);
            }
        }
    }

    private void DrainLargeIntoGroup(ref PostingList.Iterator iter, Span<long> entryBuffer)
    {
        while (true)
        {
            if (iter.Fill(entryBuffer, out int read) == false || read == 0)
                break;
            AddToGroup(entryBuffer, read);
        }
    }

    private void DrainSpecialIntoGroup(long postingListId, Span<long> entryBuffer)
    {
        var termType = (TermIdMask)postingListId & TermIdMask.EnsureIsSingleMask;
        switch (termType)
        {
            case TermIdMask.Single:
            {
                long entryId = (long)EntryIdEncodings.GetContainerId(postingListId);
                if (_emittedBitmap.Contains(entryId))
                    return;
                _emittedBitmap.Add(entryId);
                // Ensure parallel _groupSecondary/_groupSortedIndexes/_groupTerms buffers grow in step
                EnsureGroupCapacity(_groupEntries.Count + 1);
                _groupEntries.AddUnsafe(entryId);
                break;
            }
            case TermIdMask.SmallPostingList:
            {
                Container.Get(_llt, EntryIdEncodings.GetContainerId(postingListId), out var item);
                _ = VariableSizeEncoding.Read<int>(item.Address, out var offset);
                var smallReader = new FastPForBufferedReader(_llt.Allocator);
                try
                {
                    smallReader.Init(item.Address + offset, item.Length - offset);
                    DrainSmallIntoGroup(ref smallReader, entryBuffer);
                }
                finally
                {
                    if (smallReader.WasInitialized)
                        smallReader.Dispose();
                }
                break;
            }
            case TermIdMask.PostingList:
            {
                InitPostingList(out var pl, out var iter, postingListId);
                using (pl)
                    DrainLargeIntoGroup(ref iter, entryBuffer);
                break;
            }
            default:
                throw new ArgumentException($"Unexpected TermIdMask value {termType} for special posting list id {postingListId}");
        }
    }

    private void AddToGroup(Span<long> entryBuffer, int read)
    {
        EntryIdEncodings.DecodeAndDiscardFrequency(entryBuffer[..read], read);
        int newCount = _emittedBitmap.DedupAddNew(entryBuffer, read);
        if (newCount == 0)
            return;
        if (_groupEntries.Count + newCount >= _maxGroupSize)
            TruncateGroupToTopTake();
        EnsureGroupCapacity(_groupEntries.Count + newCount);
        entryBuffer[..newCount].CopyTo(
            new Span<long>(_groupEntries.RawItems + _groupEntries.Count, newCount));
        _groupEntries.Count += newCount;
    }

    /// <summary>Keep only the top N entries of the current group by secondary value, discarding the rest.</summary>
    private void TruncateGroupToTopTake()
    {
        int n = _groupEntries.Count;
        if (n <= _take)
            return;

        ResolveGroupSecondary();

        var docs = new Span<int>(_groupSortedIndexes.RawItems, _take);
        switch (_secondaryType)
        {
            case MatchCompareFieldType.Integer:
            {
                var terms = new Span<long>(_groupHeapTerms.RawItems, _take);
                var sorter = HeapSorterBuilder.BuildSingleNumericalSorter(docs, terms, _secondaryDescending, _nullIsSmallest);
                for (int i = 0; i < n; i++)
                    sorter.Insert(i, _groupSecondary.RawItems[i]);
                break;
            }
            case MatchCompareFieldType.Floating:
            {
                var terms = new Span<double>((double*)_groupHeapTerms.RawItems, _take);
                var sorter = HeapSorterBuilder.BuildSingleNumericalSorter(docs, terms, _secondaryDescending, _nullIsSmallest);
                for (int i = 0; i < n; i++)
                    sorter.Insert(i, BitConverter.Int64BitsToDouble(_groupSecondary.RawItems[i]));
                break;
            }
            default: // Sequence
            {
                var terms = new Span<UnmanagedSpan>(_groupHeapTermsSeq.RawItems, _take);
                var sorter = HeapSorterBuilder.BuildSingleCompactKeySorter(docs, terms, _secondaryDescending, _nullIsSmallest);
                for (int i = 0; i < n; i++)
                    sorter.Insert(i, _groupTerms.RawItems[i]);
                break;
            }
        }

        // Compact survivors (docs[0.._take]) to the front of _groupEntries. _groupSecondary used as scratchpad here
        var entries = _groupEntries.RawItems;
        var scratch = _groupSecondary.RawItems;
        for (int i = 0; i < _take; i++)
            scratch[i] = entries[docs[i]];
        new Span<long>(scratch, _take).CopyTo(new Span<long>(entries, _take));

        _groupSortedIndexes.Count = _groupSecondary.Count = _groupEntries.Count = _take;

        // The heap left the survivors in heap order, NOT entry-id order. The secondary lookups REQUIRES its keys sorted by entry id
        new Span<long>(entries, _take).Sort();
    }

    private void ResolveGroupSecondary()
    {
        var entriesSpan = _groupEntries.ToSpan();
        var secondarySpan = new Span<long>(_groupSecondary.RawItems, _groupSecondary.Capacity);
        if (_secondaryLookup is null)
        {
            secondarySpan[..entriesSpan.Length].Fill(_missingSecondaryValue);
            return;
        }

        int n = entriesSpan.Length;
        Debug.Assert(secondarySpan.Length >= n);
        _secondaryLookup.GetFor(entriesSpan, secondarySpan, _missingSecondaryValue);

        if (_secondaryType != MatchCompareFieldType.Sequence) 
            return; // long / double are already handled above
        
        var termsSpan = new Span<UnmanagedSpan>(_groupTerms.RawItems, _groupTerms.Capacity);
        Debug.Assert(termsSpan.Length >= n);
        SortingHelpers.ReplaceNullAndNonExistingTermIds(secondarySpan[..n], _nonExistingTermContainerId, _nullTermContainerId, _missingSecondaryValue);
        Container.GetAllSortedByPage(_llt, secondarySpan[..n], termsSpan[..n], _llt.PageLocator);
    }

    private void SortGroupBySecondary()
    {
        var entriesSpan = _groupEntries.ToSpan();
        var secondarySpan = new Span<long>(_groupSecondary.RawItems, _groupSecondary.Capacity);
        var indexesSpan = new Span<int>(_groupSortedIndexes.RawItems, _groupSortedIndexes.Capacity);
        var termsSpan = new Span<UnmanagedSpan>(_groupTerms.RawItems, _groupTerms.Capacity);

        _groupEmitIdx = 0;
        int n = entriesSpan.Length;

        Debug.Assert(secondarySpan.Length >= n);
        Debug.Assert(indexesSpan.Length >= n);
        // _groupTerms (hence termsSpan) is only allocated for the Sequence path, so its length is asserted
        // inside that branch — Integer/Floating tie-breaks never touch termsSpan.

        RoaringBitmap.InitializeIndices(indexesSpan, n);
        var idxs = indexesSpan[..n];

        if (_secondaryLookup is null)
        {
            if (_secondaryType is MatchCompareFieldType.Integer or MatchCompareFieldType.Floating)
            {
                secondarySpan[..n].Fill(_missingSecondaryValue);
            }
            else
            {
                termsSpan[..n].Clear();
            }

            return;
        } 
        
        _secondaryLookup.GetFor(entriesSpan, secondarySpan, _missingSecondaryValue);

        switch (_secondaryType)
        {
            case MatchCompareFieldType.Integer:
                secondarySpan[..n].Sort(idxs);
                break;
            case MatchCompareFieldType.Floating:
                MemoryMarshal.Cast<long, double>(secondarySpan[..n]).Sort(idxs);
                break;
            case MatchCompareFieldType.Sequence:
                Debug.Assert(termsSpan.Length >= n);
                SortingHelpers.ReplaceNullAndNonExistingTermIds(secondarySpan[..n], _nonExistingTermContainerId, _nullTermContainerId, SortingHelpers.MissingTermId);
                Container.GetAllSortedByPage(_llt, secondarySpan[..n], termsSpan[..n], _llt.PageLocator);
                fixed (UnmanagedSpan* termsPtr = termsSpan)
                {
                    idxs.Sort(new SliceComparer(termsPtr));
                }
                break;
            default:
                throw new InvalidOperationException($"Unexpected secondary type {_secondaryType}");
        }
    }
    
    private readonly struct SliceComparer(UnmanagedSpan* terms) : IComparer<int>
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Compare(int x, int y)
        {
            ref var xItem = ref terms[x];
            ref var yItem = ref terms[y];

            if (yItem.Address == null)
                return xItem.Address == null ? 0 : 1;
            if (xItem.Address == null)
                return -1;

            var cmp = Memory.Compare(xItem.Address + 1, yItem.Address + 1, Math.Min(xItem.Length - 1, yItem.Length - 1));
            if (cmp != 0)
                return cmp;

            var xBits = (xItem.Length - 1) * 8 - (xItem.Address[0] >> 4);
            var yBits = (yItem.Length - 1) * 8 - (yItem.Address[0] >> 4);
            return xBits - yBits;
        }
    }

    private int EmitFromSortedGroup(Span<long> matches)
    {
        var entries = _groupEntries.RawItems;
        var indexes = _groupSortedIndexes.RawItems;
        int remaining = _groupEntries.Count - _groupEmitIdx;
        int toEmit = Math.Min(remaining, matches.Length);
        var (pos, step) = _secondaryDescending ? ( _groupEntries.Count - 1 - _groupEmitIdx, -1) : (_groupEmitIdx, 1);
        for (int i = 0; i < toEmit; i++, pos += step)
            matches[i] = entries[indexes[pos]];
        _groupEmitIdx += toEmit;
        if (_groupEmitIdx >=  _groupEntries.Count)
            _groupEntries.Count = 0;
        return toEmit;
    }

    // Loads all null-primary docs into the group buffer and sorts them by secondary value,
    // so null-primary entries obey the same per-group secondary sort as regular terms.
    private void PrepareNullGroup(Span<long> entryBuffer)
    {
        if (_nullGroupPrepared) return;
        _nullGroupPrepared = true;

        bool hasNull = _hasNullPostingList && _nullExhausted == false;
        bool hasNonExisting = _hasNonExistingPostingList && _nonExistingExhausted == false;
        if (hasNull == false && hasNonExisting == false)
            return;

        _groupEntries.Count = 0;

        // Drain both null and non-existing entries into a single group; the secondary sort
        // determines their interleaved order within the group.
        if (hasNonExisting)
        {
            DrainSpecialIntoGroup(_nonExistingPostingListId, entryBuffer);
            _nonExistingExhausted = true;
        }

        int countAfterNonExisting = _groupEntries.Count;
        if (hasNull)
        {
            DrainSpecialIntoGroup(_nullPostingListId, entryBuffer);
            _nullExhausted = true;
        }

        if (_groupEntries.Count <= 0) return;

        if (countAfterNonExisting > 0 && _groupEntries.Count > countAfterNonExisting) 
            _groupEntries.ToSpan().Sort(); // both contributed (each separately sorted), so we have to sort

        SortGroupBySecondary();
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
        return new QueryInspectionNode("SortedDrivingWithTieBreakMatch",
            parameters: new Dictionary<string, string>
            {
                ["Provider"] = _provider.Inspect().Operation,
                ["TieBreakType"] = _secondaryType.ToString(),
                ["TieBreakDescending"] = _secondaryDescending.ToString()
            });
    }

    public void Dispose()
    {
        _emittedBitmap.Dispose();
        _plIdsBuffer.Dispose(_allocator);
        _smallContainerItems.Dispose(_allocator);
        _groupEntries.Dispose(_allocator);
        _groupSecondary.Dispose(_allocator);
        _groupSortedIndexes.Dispose(_allocator);
        _groupTerms.Dispose(_allocator);
        _groupHeapTerms.Dispose(_allocator);
        _groupHeapTermsSeq.Dispose(_allocator);
    }
}
