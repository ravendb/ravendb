using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using Corax.Querying.Matches.Meta;
using Sparrow.Server;
using Voron.Data.RoaringBitmaps;

namespace Corax.Querying.Matches;

/// <summary>
/// Lightweight IQueryMatch backed by a RoaringBitmap. Used when query operations
/// (search, OR/AND of terms) produce a bitmap result that needs to be wrapped
/// as an IQueryMatch for the rest of the pipeline.
/// </summary>
public struct BitmapMatch(ByteStringContext allocator) : IBitmapQueryMatch, IDisposable
{
    private RoaringBitmap _bitmapState = new(allocator);
    private RoaringBitmapIterator _iterator;
    private bool _iteratorInitialized = false;
    
    public bool IsAllocated => allocator != null;

    /// <summary>Get a mutable reference to the internal bitmap state for building.
    /// The returned ref is intentionally unscoped because callers thread it through
    /// QueryPrimitives.OrWithMatch / AndWithMatch chains where the BitmapMatch lives
    /// on the caller's stack frame for the full call duration. Suppresses CS9084.</summary>
    [UnscopedRef]
    public ref RoaringBitmap BitmapState => ref _bitmapState;

    public long Count => _bitmapState.ComputeCount();
    public bool IsBoosting => false;

    public long MinEntryId
    {
        get
        {
            long minKey = _bitmapState.MinContainerKey;
            return minKey < 0 ? 0 : minKey * RoaringBitmap.ContainerSize;
        }
    }

    public long MaxEntryId
    {
        get
        {
            long maxKey = _bitmapState.MaxContainerKey;
            return maxKey < 0 ? 0 : (maxKey + 1) * RoaringBitmap.ContainerSize - 1;
        }
    }

    public int Fill(Span<long> matches)
    {
        if (_iteratorInitialized is false)
        {
            _bitmapState.PrepareForReading();
            _iterator = _bitmapState.GetIterator();
            _iteratorInitialized = true;
        }
        return _iterator.Fill(ref _bitmapState, matches);
    }

    public void Score(Span<long> matches, Span<float> scores, float boostFactor)
    {
        // For bitmap-backed matches (e.g. search() results built by OR-ing term posting
        // lists), per-term BM25 frequency data is not available. Contribute a flat
        // boostFactor for each entry present in the bitmap so that query-time boost()
        // and document-level boost differentiate scores correctly.
        if (boostFactor == 0f)
            return;
        for (int i = 0; i < matches.Length; i++)
        {
            if (_bitmapState.Contains(matches[i]))
                scores[i] += boostFactor;
        }
    }

    // matches is sorted ascending here (in-memory score sort), so let the bitmap group by container and merge
    // each sorted Array container with a single forward cursor instead of an independent search per probe.
    public void ScoreSorted(Span<long> matches, Span<float> scores, float boostFactor)
        => _bitmapState.ScorePresentSorted(matches, scores, boostFactor);


    public QueryInspectionNode Inspect()
    {
        return new QueryInspectionNode(nameof(BitmapMatch),
            parameters: new Dictionary<string, string>
            {
                { "Count", Count.ToString() }
            });
    }

    public void Dispose()
    {
        if (_iteratorInitialized)
            _iterator.Dispose();
        _bitmapState.Dispose();
    }
}
