using System;
using System.Linq;
using Corax;
using Corax.Indexing;
using Corax.Mappings;
using Corax.Querying;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Matches.SortingMatches.Meta;
using Corax.Utils;
using FastTests.Voron;
using Sparrow;
using Tests.Infrastructure;
using Voron;
using Xunit;

namespace SlowTests.Corax.Bugs;

public class RavenDB_25410(ITestOutputHelper output) : StorageTest(output)
{
    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void BinaryMatchProperlyRetrievesScores()
    {
        using var mapping = GetMappingAndIndexDocuments();
        using var searcher = new IndexSearcher(Env, mapping);
        var startsWith = searcher.StartWithQuery("id()", "t", hasBoost: true);
        var dummyMatch = new DummyMatch();
        
        Span<long> ids = stackalloc long[32];
        Span<float> scores = stackalloc float[32];
        scores.Fill(float.Epsilon);
        var queryToRun = searcher.And(startsWith, dummyMatch);
        var offset = 0;
        while (queryToRun.Fill(ids.Slice(offset, 2)) is var read and > 0)
            offset += read;
        Assert.Equal(16, offset);
        
        queryToRun.Score(ids, scores, 1);
        Assert.Equal(ids.Slice(0, 16).ToArray().Select(x => (float)x), scores.Slice(0, 16).ToArray());
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
                var q = indexSearcher.OrderBy(dummyMatch, [new OrderMetadata(true, MatchCompareFieldType.Score), new OrderMetadata(mapping.GetByFieldId(0).Metadata, true, MatchCompareFieldType.Sequence, fieldHasNoTerms: false)], nullFirst: true);
                q.SetSortingDataTransfer(transfer);
                sortingMatch = q;
            }
            else
            {
                var q = indexSearcher.OrderBy(dummyMatch, new OrderMetadata(true, MatchCompareFieldType.Score), nullFirst: true);
                q.SetScoreAndDistanceBuffer(transfer);
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
        public SkipSortingResult AttemptToSkipSorting() => SkipSortingResult.SortingIsRequired;

        public QueryCountConfidence Confidence => QueryCountConfidence.Low;
        public bool IsBoosting { get => true; }

        public int Fill(Span<long> matches)
        {
            _fillExecuted = true;
            ref var count = ref _count;
            var toReturn = Math.Min(8, Math.Abs(_count - 16));
            if (toReturn == 0)
                return 0;

            for (int currentId = count; currentId < count + toReturn; currentId++)
            {
                matches[currentId - count] = currentId + 1;
            }

            count += toReturn;
            return toReturn;
        }

        public int AndWith(Span<long> buffer, int matches) => throw new NotImplementedException();


        public void Score(Span<long> matches, Span<float> scores, float boostFactor)
        {
            Assert.True(_fillExecuted);
            for (int i = 0; i < matches.Length; i++)
            {
                scores[i] = (float)matches[i];
            }
        }

        public QueryInspectionNode Inspect() => throw new NotImplementedException();

        public DuplicatesOccurrence DuplicatesOccurrenceStatus => DuplicatesOccurrence.Possible;
    }
}
