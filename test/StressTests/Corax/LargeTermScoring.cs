using System;
using System.Linq;
using System.Text;
using Corax;
using Corax.Indexing;
using Corax.Mappings;
using Corax.Querying;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using FastTests.Voron;
using Tests.Infrastructure;
using Xunit;

namespace StressTests.Corax;

public class LargeTermScoring(ITestOutputHelper output) : StorageTest(output)
{
    // More documents than Bm25Relevance keeps in memory (104,857), so the term is scored by reading its posting list again.
    private const int NumberOfEntries = 150_000;
    private const int EntriesWithTerm = 120_000;

    [RavenFact(RavenTestCategory.Corax)]
    public void EveryDocumentOfTermWithLargePostingListIsScored()
    {
        using var mapping = IndexEntries();
        using var searcher = new IndexSearcher(Env, mapping);
        var match = Term(searcher);
        var ids = FillAll(ref match);

        Assert.Equal(EntriesWithTerm, ids.Length);

        var scores = new float[ids.Length];
        match.Score(ids, scores, 1f);

        Assert.Equal(EntriesWithTerm, scores.Count(score => score > 0));
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void SubsetOfMatchesGetsSameScoresAsAllMatches()
    {
        using var mapping = IndexEntries();
        using var searcher = new IndexSearcher(Env, mapping);
        var all = Term(searcher);
        var ids = FillAll(ref all);
        var scores = new float[ids.Length];
        all.Score(ids, scores, 1f);

        // From the middle of the posting list, so the postings before and after the subset are skipped.
        var positions = Enumerable.Range(40_000, 40_000).Where(i => i % 3 == 0).ToArray();
        var subset = Term(searcher);
        FillAll(ref subset);
        var subsetScores = new float[positions.Length];
        subset.Score(positions.Select(i => ids[i]).ToArray(), subsetScores, 1f);

        Assert.Equal(positions.Select(i => scores[i]), subsetScores);
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void ScoresMatchRecordedValues()
    {
        using var mapping = IndexEntries();
        using var searcher = new IndexSearcher(Env, mapping);
        var term = Term(searcher);
        var termIds = FillAll(ref term);
        var allEntries = searcher.AllEntries();
        var allIds = FillAll(ref allEntries);

        (string Scenario, long[] Matches)[] scenarios =
        [
            ("AllEntries", allIds),
            ("TermDocuments", termIds),
            ("MiddleRange", allIds[60_000..90_000]),
            ("Head", allIds[..10_000]),
            ("Tail", allIds[^10_000..]),
            ("Spread", allIds.Where((_, i) => i % 7 == 0).ToArray())
        ];

        (string Scenario, int Scored, ulong Checksum)[] expected =
        [
            ("AllEntries", 120000, 14515089610702163109UL),
            ("TermDocuments", 120000, 7111085803595306597UL),
            ("MiddleRange", 24000, 15596193201234367141UL),
            ("Head", 8000, 13488463198898395557UL),
            ("Tail", 8000, 13488463198898395557UL),
            ("Spread", 17143, 7026112444919711872UL)
        ];

        var actual = scenarios.Select(x =>
        {
            var scores = Score(searcher, x.Matches);
            var checksum = 14695981039346656037UL;
            foreach (var score in scores)
                checksum = (checksum ^ (uint)BitConverter.SingleToInt32Bits(score)) * 1099511628211UL;

            return (x.Scenario, scores.Count(score => score > 0), checksum);
        }).ToArray();

        Assert.Equal(expected, actual);
    }

    private static float[] Score(IndexSearcher searcher, long[] matches)
    {
        var term = Term(searcher);
        FillAll(ref term);
        var scores = new float[matches.Length];
        term.Score(matches, scores, 1f);
        return scores;
    }

    private IndexFieldsMapping IndexEntries()
    {
        var mapping = IndexFieldsMappingBuilder.CreateForWriter(false).AddBinding(0, "Id").AddBinding(1, "Status").Build();
        using var writer = new IndexWriter(Env, mapping, SupportedFeatures.All);
        for (var i = 0; i < NumberOfEntries; i++)
        {
            using var builder = writer.Index($"entries/{i}");
            builder.Write(0, Encoding.UTF8.GetBytes($"entries/{i}"));
            if (i % 5 == 0)
            {
                builder.Write(1, "inactive"u8);
            }
            else
            {
                for (var occurrence = 0; occurrence <= i % 4; occurrence++)
                    builder.Write(1, "active"u8);
            }

            builder.EndWriting();
        }

        writer.Commit();
        return mapping;
    }

    private static TermMatch Term(IndexSearcher searcher) => searcher.TermQuery(searcher.FieldMetadataBuilder("Status", 1, hasBoost: true), "active");

    private static long[] FillAll<TMatch>(ref TMatch match) where TMatch : IQueryMatch
    {
        var ids = new long[NumberOfEntries];
        var read = 0;
        while (match.Fill(ids.AsSpan(read)) is var count and > 0)
            read += count;

        return ids[..read];
    }
}
