using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Corax.Mappings;
using Corax.Querying.Matches.Meta;
using Corax.Utils;
using Voron.Data.PostingLists;

namespace Corax.Querying.Matches;

public struct IncludeNullMatch<TInner> : IQueryMatch
where TInner : IQueryMatch
{
    private readonly bool _forward;
    private bool _hasLeftNulls;
    private bool _innerEnd = false;
    private PostingList.Iterator _postingListIterator;
    public IncludeNullMatch(Querying.IndexSearcher searcher, in TInner inner, in FieldMetadata field, bool forward)
    {
        _forward = forward;
        _inner = inner;
        
        _hasLeftNulls = searcher.TryGetPostingListForNull(in field, out var postingListId);
        if (_hasLeftNulls)
            _postingListIterator = searcher.GetPostingList(postingListId).Iterate();
    }
    
    private TInner _inner;
    
    public long Count { get; }

    public SkipSortingResult AttemptToSkipSorting()
    {
        return SkipSortingResult.WillSkipSorting;
    }

    public QueryCountConfidence Confidence { get; }
    public bool IsBoosting { get; }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int Fill(Span<long> matches)
    {
        return _forward ? 
            ForwardStreaming(matches) 
            : BackwardStreaming(matches);
    }

    private int BackwardStreaming(Span<long> matches)
    {
        int read;
        if (_innerEnd == false)
        {
            read = _inner.Fill(matches);
            if (read > 0)
                return read;
            _innerEnd = true;
        }

        FetchNulls(matches, out read);
        return read;
    }

    private int ForwardStreaming(Span<long> matches)
    {
        if (FetchNulls(matches, out int read))
        {
            return read;
        }

        return _inner.Fill(matches);
    }

    private bool FetchNulls(Span<long> matches, out int matchesCount)
    {
        if (_hasLeftNulls)
        {
            if (_postingListIterator.Fill(matches, out var read))
            {
                EntryIdEncodings.DecodeAndDiscardFrequency(matches, read);
                matchesCount = read;
                return true;
            }

            _hasLeftNulls = false;
        }

        matchesCount = 0;
        return false;
    }

    public int AndWith(Span<long> buffer, int matches)
    {
        throw new NotImplementedException($"{nameof(IncludeNullMatch<TInner>)} is made for streaming operation. Binary operation is not supported");
    }

    public void Score(Span<long> matches, Span<float> scores, float boostFactor)
    {
        throw new NotImplementedException($"{nameof(IncludeNullMatch<TInner>)} is made for streaming operation. Boosting is not supported");
    }

    public QueryInspectionNode Inspect()
    {
        return new QueryInspectionNode(nameof(IncludeNullMatch<TInner>),
            children: new List<QueryInspectionNode>(){_inner.Inspect()},
            parameters: new Dictionary<string, string>());
    }
}
