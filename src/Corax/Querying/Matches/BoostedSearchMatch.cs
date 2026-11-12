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
/// A boosted `search()` over several terms combined with OR or AND, relevance is preserved by retaining each per-term scored
/// so we can sum their BM25 contributions properly.
/// </summary>
public sealed class BoostedSearchMatch(ByteStringContext allocator, List<IQueryMatch> terms, Constants.Search.Operator @operator, CancellationToken token = default)
    : IBitmapQueryMatch, IDisposable
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

    public void Score(Span<long> matches, Span<float> scores, float boostFactor) => Score(matches, scores, boostFactor, sorted: false);

    public void ScoreSorted(Span<long> matches, Span<float> scores, float boostFactor) => Score(matches, scores, boostFactor, sorted: true);

    private void Score(Span<long> matches, Span<float> scores, float boostFactor, bool sorted)
    {
        EnsureInitialized();
        if (@operator == Constants.Search.Operator.Or)
        {
            ScoreTerms(matches, scores, boostFactor, sorted);
            return;
        }
        
        using var _ = allocator.Allocate(matches.Length, out Span<float> andScores);
        andScores.Clear();
        ScoreTerms(matches, andScores, boostFactor, sorted);

        for (int i = 0; i < matches.Length; i++)
        {
            if (_bitmap.Contains(matches[i]))
                scores[i] += andScores[i];
        }
    }

    private void ScoreTerms(Span<long> matches, Span<float> scores, float boostFactor, bool sorted)
    {
        for (int i = 0; i < terms.Count; i++)
        {
            if (sorted)
                terms[i].ScoreSorted(matches, scores, boostFactor);
            else
                terms[i].Score(matches, scores, boostFactor);
        }
    }

    public QueryInspectionNode Inspect()
    {
        return new QueryInspectionNode($"{nameof(BoostedSearchMatch)} [{@operator}]",
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
        var tempBitmap = new RoaringBitmap(allocator);
        for (int i = 0; i < terms.Count; i++)
        {
            if (i == 0 || @operator == Constants.Search.Operator.Or)
                QueryPrimitives.OrWithMatch(terms[i], ref _bitmap, token: token);
            else
                QueryPrimitives.AndWithMatch(terms[i], ref _bitmap, ref tempBitmap, token);
        }
        tempBitmap.Dispose();

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
