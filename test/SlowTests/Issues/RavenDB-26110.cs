using System;
using System.Collections.Generic;
using Corax.Mappings;
using Corax.Querying.Matches.Meta;
using FastTests.Voron;
using Sparrow.Server;
using Tests.Infrastructure;
using Voron;
using Xunit;
using Xunit.Abstractions;
using IndexSearcher = Corax.Querying.IndexSearcher;

namespace SlowTests.Issues;

public class RavenDB_26110(ITestOutputHelper output) : StorageTest(output)
{
    [RavenMultiplatformFact(RavenTestCategory.Corax, RavenArchitecture.AllX64)]
    public void MemoizationMatchProviderCanMemoizeLargeResultSet()
    {
        using var fields = CreateFields(Allocator);
        using var searcher = new IndexSearcher(Env, fields);

        searcher.MaxMemoizationSizeInBytes = long.MaxValue;

        var mock = new DummyBigMatch();
        var memoizer = searcher.Memoize(mock).Replay();
        var results = memoizer.FillAndRetrieve();
        Assert.Equal(DummyBigMatch.TotalItemsToProduce, results.Length);
        for (int i = 1; i < Math.Min(1000, results.Length); i++)
        {
            Assert.True(results[i] > results[i - 1]);
        }
    }

    [RavenMultiplatformFact(RavenTestCategory.Corax, RavenArchitecture.AllX64)]
    public void MemoizationMatchProviderThrowsWhenResultSetExceedsBuffer()
    {
        using var fields = CreateFields(Allocator);
        using var searcher = new IndexSearcher(Env, fields);

        searcher.MaxMemoizationSizeInBytes = 16 * 1024 * 1024;

        var mock = new DummyBigMatch();
        var memoizer = searcher.Memoize(mock).Replay();
        Assert.Throws<InvalidOperationException>(() => memoizer.FillAndRetrieve());
    }

    private static IndexFieldsMapping CreateFields(ByteStringContext bsc)
    {
        Slice.From(bsc, "Id", ByteStringType.Immutable, out var idSlice);

        using var builder = IndexFieldsMappingBuilder.CreateForWriter(false)
            .AddBinding(0, idSlice);

        return builder.Build();
    }

    private struct DummyBigMatch : IQueryMatch
    {
        public const int TotalItemsToProduce = 200_000_000;

        private long _totalGenerated;
        private bool _completed;

        public long Count => 1000;
        public QueryCountConfidence Confidence => QueryCountConfidence.High;
        public bool IsBoosting => false;
        public DuplicatesOccurrence DuplicatesOccurrenceStatus => DuplicatesOccurrence.NotPossible;

        public SkipSortingResult AttemptToSkipSorting() => SkipSortingResult.WillSkipSorting;

        public int Fill(Span<long> matches)
        {
            if (_completed) return 0;

            int fillCount = (int)Math.Min(Math.Min(matches.Length, 10_000_000), TotalItemsToProduce - _totalGenerated);

            long baseId = _totalGenerated + 1;
            for (int i = 0; i < fillCount; i++)
            {
                matches[i] = baseId + i;
            }

            _totalGenerated += fillCount;
            if (_totalGenerated >= TotalItemsToProduce)
                _completed = true;

            return fillCount;
        }

        public int AndWith(Span<long> buffer, int matches) => matches;

        public void Score(Span<long> matches, Span<float> scores, float boostFactor)
        {
        }

        public QueryInspectionNode Inspect() => new(nameof(DummyBigMatch),
            parameters: new Dictionary<string, string>
            {
                { "TotalGenerated", _totalGenerated.ToString() },
                { "Count", Count.ToString() }
            });
    }
}
