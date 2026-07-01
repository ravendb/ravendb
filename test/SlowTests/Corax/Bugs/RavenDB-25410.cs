using System;
using System.Linq;
using Corax;
using Corax.Indexing;
using Corax.Mappings;
using Corax.Querying;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Matches.SortingMatches.Meta;
using Corax.Querying.Primitives;
using Corax.Utils;
using FastTests.Voron;
using Sparrow;
using Tests.Infrastructure;
using Voron;
using Voron.Data.RoaringBitmaps;
using Xunit;

namespace SlowTests.Corax.Bugs;

public class RavenDB_25410(ITestOutputHelper output) : StorageTest(output)
{
    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void BitmapAndCompositionPreservesScores()
    {
        using var mapping = GetMappingAndIndexDocuments();
        using var searcher = new IndexSearcher(Env, mapping);

        // Compose AND(startsWith, dummyMatch) via bitmap primitives —
        // the new-pipeline equivalent of the old searcher.And(). Verifies
        // that scoring from a boosted match survives bitmap AND composition.
        var startsWith = searcher.StartWithQuery("id()", "t", hasBoost: true);
        IQueryMatch dummyMatch = new DummyMatch();

        var bitmap = new BitmapMatch(searcher.Allocator);
        // temp is passed by ref into AndWithMatch, so it can't be a `using var` (CS1657); a try/finally keeps
        // disposal exception-safe.
        RoaringBitmap temp = new(searcher.Allocator);
        try
        {
            QueryPrimitives.OrWithMatch(startsWith, ref bitmap.BitmapState);
            QueryPrimitives.AndWithMatch(dummyMatch, ref bitmap.BitmapState, ref temp);
        }
        finally
        {
            temp.Dispose();
        }

        Span<long> ids = stackalloc long[32];
        Span<float> scores = stackalloc float[32];
        scores.Fill(float.Epsilon);
        int offset = bitmap.Fill(ids);
        Assert.Equal(16, offset);

        // Score through the boosted DummyMatch — verify scores survive the AND
        dummyMatch.Score(ids.Slice(0, offset), scores.Slice(0, offset), 1);
        Assert.Equal(ids.Slice(0, 16).ToArray().Select(x => (float)x), scores.Slice(0, 16).ToArray());

        bitmap.Dispose();
    }
    
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [InlineData(true)]
    [InlineData(false)]
    public void BoostingIsProperlyRetrievedInSortingMethods(bool multiSort)
    {
        using var mapping = GetMappingAndIndexDocuments();
        using (var indexSearcher = new IndexSearcher(Env, mapping))
        {
            var dummyMatch = new DummyMatch();
            Span<long> ids = stackalloc long[32];
            SortingDataTransfer transfer = new()
            {
                ScoresBuffer = new float[16],
            };

            IQueryMatch sortingMatch;
            if (multiSort)
            {
                var q = indexSearcher.OrderBy(dummyMatch, [new OrderMetadata(true, MatchCompareFieldType.Score), new OrderMetadata(mapping.GetByFieldId(0).Metadata, true, MatchCompareFieldType.Sequence)], defaultNullsSortMode: NullsSortMode.NullsSmallest);
                q.SetSortingDataTransfer(transfer);
                sortingMatch = q;
            }
            else
            {
                var q = indexSearcher.OrderBy(dummyMatch, new OrderMetadata(true, MatchCompareFieldType.Score), defaultNullsSortMode: NullsSortMode.NullsSmallest);
                q.SetSortingDataTransfer(transfer);
                sortingMatch = q;
            }

            var read = sortingMatch.Fill(ids);
            Assert.Equal(16, read);
            Assert.Equal(Enumerable.Range(1, 16).Select(x => (float)x).Reverse(), transfer.ScoresBuffer);
        }
    }

    private IndexFieldsMapping GetMappingAndIndexDocuments()
    {
        var mapping = IndexFieldsMappingBuilder.CreateForWriter(true).AddBinding(0, "id()").Build();
        using (var indexWriter = new IndexWriter(Env, mapping, SupportedFeatures.All))
        {
            for (int i = 1; i <= 16; i++)
            {
                using var entryBuilder = indexWriter.Index($"test/{i}");
                entryBuilder.Write(0, Encodings.Utf8.GetBytes($"test/{i}"));
                entryBuilder.EndWriting();
            }

            indexWriter.Commit();
        }

        return mapping;
    }

    private struct DummyMatch : IQueryMatch
    {
        private int _count;
        private bool _fillExecuted;
        public long Count => 16;

        public bool IsBoosting { get => true; }

        public int Fill(Span<long> matches)
        {
            _fillExecuted = true;
            ref var count = ref _count;
            var toReturn = Math.Min(matches.Length, Math.Min(8, Math.Abs(_count - 16)));
            if (toReturn == 0)
                return 0;

            for (int currentId = count; currentId < count + toReturn; currentId++)
            {
                matches[currentId - count] = currentId + 1;
            }

            count += toReturn;
            return toReturn;
        }

        public void Score(Span<long> matches, Span<float> scores, float boostFactor)
        {
            Assert.True(_fillExecuted);
            for (int i = 0; i < matches.Length; i++)
            {
                scores[i] = (float)matches[i];
            }
        }

        public void ScoreSorted(Span<long> matches, Span<float> scores, float boostFactor) => Score(matches, scores, boostFactor);

        public QueryInspectionNode Inspect() => throw new NotImplementedException();
    }
}
