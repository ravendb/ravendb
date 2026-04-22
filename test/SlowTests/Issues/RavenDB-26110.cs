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
        Assert.True(results.Length > 1_000_000);
        for (int i = 1; i < Math.Min(1000, results.Length); i++)
        {
            Assert.True(results[i] > results[i - 1]);
        }
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
        private int _fillCallCount;
        private bool _completed;

        public long Count => int.MaxValue;
        public QueryCountConfidence Confidence => QueryCountConfidence.High;
        public bool IsBoosting => false;
        public DuplicatesOccurrence DuplicatesOccurrenceStatus => DuplicatesOccurrence.NotPossible;

        public SkipSortingResult AttemptToSkipSorting() => SkipSortingResult.WillSkipSorting;

        public int Fill(Span<long> matches)
        {
            if (_completed) return 0;

            _fillCallCount++;

            var fillCount = Math.Min(matches.Length - 1, 90_000_000);

            long baseId = (_fillCallCount - 1L) * fillCount + 1;
            for (int i = 0; i < fillCount; i++)
            {
                matches[i] = baseId + i;
            }

            if (_fillCallCount >= 20)
            {
                _completed = true;
                return fillCount / 2;
            }

            return fillCount;
        }

        public int AndWith(Span<long> buffer, int matches) => matches;

        public void Score(Span<long> matches, Span<float> scores, float boostFactor)
        {
        }

        public QueryInspectionNode Inspect() => new(nameof(DummyBigMatch),
            parameters: new Dictionary<string, string>
            {
                { "FillCallCount", _fillCallCount.ToString() },
                { "Count", Count.ToString() }
            });
    }
}
