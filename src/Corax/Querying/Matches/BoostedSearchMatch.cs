using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Primitives;
using Sparrow.Server;
using Voron.Data.RoaringBitmaps;

namespace Corax.Querying.Matches;

/// <summary>
/// A boosted `search()` over several terms combined with OR, relevance is preserved by retaining each per-term scored
/// so we can sum their BM25 contributions properly.
/// </summary>
public sealed class BoostedSearchMatch(ByteStringContext allocator, TermMatch[] terms, CancellationToken token = default) : IBitmapQueryMatch, IDisposable
{
    private RoaringBitmap _bitmap;
    private RoaringBitmapIterator _iterator;
    private bool _initialized;
    private long _count;

    public bool IsBoosting => true;

    public long Count
    {
        get
        {
            EnsureInitialized();
            return _count;
        }
    }

    public long MinEntryId
    {
        get
        {
            EnsureInitialized();
            long minKey = _bitmap.MinContainerKey;
            return minKey < 0 ? 0 : minKey * RoaringBitmap.ContainerSize;
        }
    }

    public long MaxEntryId
    {
        get
        {
            EnsureInitialized();
            long maxKey = _bitmap.MaxContainerKey;
            return maxKey < 0 ? 0 : (maxKey + 1) * RoaringBitmap.ContainerSize - 1;
        }
    }

    public ref RoaringBitmap BitmapState
    {
        get
        {
            EnsureInitialized();
            return ref _bitmap;
        }
    }

    public int Fill(Span<long> matches)
    {
        EnsureInitialized();
        return _iterator.Fill(ref _bitmap, matches);
    }

    public void Score(Span<long> matches, Span<float> scores, float boostFactor)
    {
        // The terms are Fill'd during Initialize, which populates their BM25 state. Each term then adds its
        // contribution for the documents it contains, leaving the rest untouched - so the total is the sum of
        // per-term relevance, mirroring how the pipeline sums scores across separate clauses.
        EnsureInitialized();
        for (int i = 0; i < terms.Length; i++)
        {
            terms[i].Score(matches, scores, boostFactor);
        }
    }

    public void ScoreSorted(Span<long> matches, Span<float> scores, float boostFactor)
    {
        EnsureInitialized();
        for (int i = 0; i < terms.Length; i++)
        {
            terms[i].ScoreSorted(matches, scores, boostFactor);
        }
    }

    public QueryInspectionNode Inspect()
    {
        return new QueryInspectionNode($"{nameof(BoostedSearchMatch)} [Or]",
            parameters: new Dictionary<string, string>
            {
                { Constants.QueryInspectionNode.IsBoosting, IsBoosting.ToString() },
                { Constants.QueryInspectionNode.Count, _initialized ? _count.ToString() : "lazy" },
            });
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void EnsureInitialized()
    {
        if (_initialized == false)
            Initialize();
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void Initialize()
    {
        _initialized = true;

        _bitmap = new RoaringBitmap(allocator);
        for (int i = 0; i < terms.Length; i++)
        {
            QueryPrimitives.OrWithMatch(terms[i], ref _bitmap, token: token);
        }

        _bitmap.PrepareForReading();
        _count = _bitmap.ComputeCount(); // cache: the pipeline may consume (steal containers from) the bitmap before Inspect()
        _iterator = _bitmap.GetIterator();
    }

    public void Dispose()
    {
        if (_initialized == false)
            return;

        _iterator.Dispose();
        _bitmap.Dispose();
    }
}
