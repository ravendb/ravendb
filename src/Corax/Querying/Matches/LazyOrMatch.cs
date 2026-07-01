using System;
using System.Collections.Generic;
using System.Threading;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Primitives;
using Sparrow.Server;
using Voron.Data.RoaringBitmaps;

namespace Corax.Querying.Matches;

public sealed class LazyOrMatch(ByteStringContext allocator, IQueryMatch left, IQueryMatch right, CancellationToken token = default) : IBitmapQueryMatch, IDisposable
{
    private RoaringBitmap _bitmap = new(allocator);
    private RoaringBitmapIterator _iterator;
    private bool _initialized;

    public bool IsBoosting => false;

    public long Count
    {
        get
        {
            Initialize();
            return _bitmap.ComputeCount();
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

    public void Score(Span<long> matches, Span<float> scores, float boostFactor)
    {
        if (boostFactor == 0f)
            return;
        Initialize();
        for (int i = 0; i < matches.Length; i++)
        {
            if (_bitmap.Contains(matches[i]))
                scores[i] += boostFactor;
        }
    }

    public void ScoreSorted(Span<long> matches, Span<float> scores, float boostFactor)
    {
        if (boostFactor == 0f)
            return;
        Initialize();
        _bitmap.ScorePresentSorted(matches, scores, boostFactor);
    }

    public QueryInspectionNode Inspect()
    {
        return new QueryInspectionNode($"{nameof(LazyOrMatch)} [Or]",
            children: [left.Inspect(), right.Inspect()],
            parameters: new Dictionary<string, string>
            {
                { Constants.QueryInspectionNode.IsBoosting, IsBoosting.ToString() },
                { Constants.QueryInspectionNode.Count, _initialized ? Count.ToString() : "lazy" },
            });
    }

    private void Initialize()
    {
        if (_initialized)
            return;
        _bitmap.Clear();
        QueryPrimitives.OrWithMatch(left, ref _bitmap, token: token);
        QueryPrimitives.OrWithMatch(right, ref _bitmap, token: token);
        _bitmap.PrepareForReading();
        _iterator = _bitmap.GetIterator();
        _initialized = true;
    }

    public void Dispose()
    {
        if (_initialized)
            _iterator.Dispose();
        _bitmap.Dispose();
        (left as IDisposable)?.Dispose();
        (right as IDisposable)?.Dispose();
    }
}
