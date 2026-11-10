using System;
using System.Buffers;
using System.Runtime.InteropServices;
using System.Threading;
using Corax.Indexing;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Primitives;
using Corax.Utils;
using Sparrow.Server;
using Voron.Data.RoaringBitmaps;
using Voron.Impl;

namespace Corax.Querying.Matches;

/// <summary>
/// Lazily fills a RoaringBitmap from the ITermsProvider on first Fill() call, then iterates the bitmap for subsequent fills.
/// Created by IndexSearcher factory methods (StartsWithQuery, EndsWithQuery, InQuery, ExistsQuery, RegexQuery, range queries, etc.) for use outside the CompiledQueryMatch pipeline.
/// </summary>
public sealed class TermsProviderMatch(ITermsProvider provider, LowLevelTransaction llt, ByteStringContext allocator, CancellationToken token = default, IndexSearcher scoringSearcher = null, double averageTermLength = 0) : IBitmapQueryMatch, IDisposable
{
    public ITermsProvider Provider => provider;

    public LowLevelTransaction Llt => llt;
    private RoaringBitmap _bitmap = new(allocator);
    private RoaringBitmapIterator _iterator;
    private bool _initialized;
    private long _count;
    private TermRelevance[] _terms;
    private int _termsCount;

    public bool IsBoosting => scoringSearcher != null;

    public long Count
    {
        get
        {
            Initialize();
            return _count;
        }
    }

    public long MinEntryId
    {
        get
        {
            Initialize();
            long minKey = _bitmap.MinContainerKey;
            return minKey < 0 ? 0 : minKey * RoaringBitmap.ContainerSize;
        }
    }

    public long MaxEntryId
    {
        get
        {
            Initialize();
            long maxKey = _bitmap.MaxContainerKey;
            return maxKey < 0 ? 0 : (maxKey + 1) * RoaringBitmap.ContainerSize - 1;
        }
    }

    public ref RoaringBitmap BitmapState
    {
        get
        {
            Initialize();
            return ref _bitmap;
        }
    }

    public int Fill(Span<long> matches)
    {
        Initialize();
        return _iterator.Fill(ref _bitmap, matches);
    }

    public void ScoreSorted(Span<long> matches, Span<float> scores, float boostFactor)
    {
        if (boostFactor == 0f || TryScoreTerms(matches, scores, boostFactor, sorted: true))
            return;
        Initialize();
        _bitmap.ScorePresentSorted(matches, scores, boostFactor);
    }

    public void Score(Span<long> matches, Span<float> scores, float boostFactor)
    {
        if (boostFactor == 0f || TryScoreTerms(matches, scores, boostFactor, sorted: false))
            return;
        Initialize();
        for (int i = 0; i < matches.Length; i++)
        {
            if (_bitmap.Contains(matches[i]))
                scores[i] += boostFactor;
        }
    }

    public QueryInspectionNode Inspect()
    {
        return provider.Inspect();
    }

    private void Initialize()
    {
        if (_initialized)
            return;
        _bitmap.Clear();
        if (scoringSearcher is null)
            QueryPrimitives.FillBitmapFromTreeScan(provider, llt, ref _bitmap, token);
        else
            FillScoredTerms();
        _bitmap.PrepareForReading();
        _count = _bitmap.ComputeCount(); // keep the count as a field, because we may consume the bitmap by time we call Inspect()
        _iterator = _bitmap.GetIterator();
        _initialized = true;
    }

    // A term with a single document is kept inline, the others keep the BM25 state their TermMatch gathered while filling the bitmap
    private void FillScoredTerms()
    {
        using var _ = allocator.Allocate(QueryPrimitives.FillBufferSize * (sizeof(long) + sizeof(int)), out ByteString buffer);
        var termIds = MemoryMarshal.Cast<byte, long>(buffer.ToSpan())[..QueryPrimitives.FillBufferSize];
        var termLengths = averageTermLength == 0
            ? Span<int>.Empty
            : MemoryMarshal.Cast<byte, int>(buffer.ToSpan()[(QueryPrimitives.FillBufferSize * sizeof(long))..]);
        int read;
        while ((read = provider.FillPostingListIds(termIds, termLengths)) > 0)
        {
            for (int i = 0; i < read; i++)
            {
                token.ThrowIfCancellationRequested();
                double termRatio = averageTermLength == 0 ? 1 : termLengths[i] / averageTermLength;
                if ((termIds[i] & (long)TermIdMask.EnsureIsSingleMask) == (long)TermIdMask.Single)
                {
                    var (entryId, frequency) = EntryIdEncodings.Decode(termIds[i]);
                    _bitmap.Add((long)entryId);
                    AddTerm(new TermRelevance { EntryId = (long)entryId, Frequency = frequency, Denominator = Bm25Relevance.Denominator((float)termRatio) });
                    continue;
                }

                var term = scoringSearcher.TermQuery(termIds[i], termRatio, isBoosting: true);
                QueryPrimitives.OrWithMatch(term, ref _bitmap, token: token);
                AddTerm(new TermRelevance { Relevance = term.Relevance });
            }
        }
    }

    private void AddTerm(in TermRelevance term)
    {
        if (_termsCount == (_terms?.Length ?? 0))
        {
            var grown = RelevancePool.Rent(Math.Max(64, 2 * _termsCount));
            if (_terms != null)
            {
                _terms.AsSpan(0, _termsCount).CopyTo(grown);
                RelevancePool.Return(_terms, clearArray: true);
            }
            _terms = grown;
        }

        _terms[_termsCount++] = term;
    }

    private static ArrayPool<TermRelevance> RelevancePool => Bm25Relevance.RelevancePool ??= ArrayPool<TermRelevance>.Create();

    private bool TryScoreTerms(Span<long> matches, Span<float> scores, float boostFactor, bool sorted)
    {
        if (scoringSearcher is null)
            return false;

        Initialize();
        float singleTermIdf = Bm25Relevance.ComputeIdf(scoringSearcher, 1);
        foreach (var term in _terms.AsSpan(0, _termsCount))
        {
            token.ThrowIfCancellationRequested();
            if (term.Relevance != null)
            {
                if (sorted)
                    term.Relevance.ScoreSorted(matches, scores, boostFactor);
                else
                    term.Relevance.Score(matches, scores, boostFactor);
            }
            else if ((sorted ? matches.BinarySearch(term.EntryId) : matches.IndexOf(term.EntryId)) is var position and >= 0)
                Bm25Relevance.ScoreSingle(matches.Slice(position, 1), scores.Slice(position, 1), boostFactor, term, singleTermIdf);
        }

        return true;
    }

    public void Dispose()
    {
        if (_terms != null)
        {
            foreach (var term in _terms.AsSpan(0, _termsCount))
                term.Relevance?.Dispose();
            RelevancePool.Return(_terms, clearArray: true);
            _terms = null;
        }
        if (_initialized)
            _iterator.Dispose();
        _bitmap.Dispose();
    }
}
