using System;
using System.Collections.Generic;
using System.Linq;
using Corax;
using Corax.Mappings;
using Corax.Querying.Matches.Meta;
using FastTests.Voron;
using Raven.Server.Documents.Indexes.Persistence.Corax;
using Sparrow.Server;
using Tests.Infrastructure;
using Voron;
using Xunit;
using IndexSearcher = Corax.Querying.IndexSearcher;
using IndexWriter = Corax.Indexing.IndexWriter;

namespace FastTests.Corax;

// A Corax query whose match is a single term, or the memoized all-entries match of a query without a where clause,
// takes its total from the match's count instead of reading every entry. These tests pin that the count equals what
// reading the match returns, for a term stored inline, in a small posting list and in a large one, after updates move
// entries between terms, after deletes, and after a term loses every entry; and that every other match is refused,
// because composite matches report only an estimate.
public class ExactMatchCountTests : StorageTest
{
    private const int IdField = 0;
    private const int StatusField = 1;

    public ExactMatchCountTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void TermAndAllEntriesCounts_EqualWhatReadingTheMatchReturns_AfterUpdatesAndDeletes()
    {
        var fields = CreateFields(Allocator);
        var statuses = new Dictionary<string, string>();

        // three commits, so the posting lists are written more than once: "large" ends up in a posting list, "small" in a
        // small posting list, "single" holds one entry and "gone" is emptied later
        for (var batch = 0; batch < 3; batch++)
        {
            using (var indexWriter = new IndexWriter(Env, fields, SupportedFeatures.All))
            {
                for (var i = batch * 10_000; i < (batch + 1) * 10_000; i++)
                {
                    var status = i == 0 ? "single" : i % 1000 == 1 ? "small" : i % 500 == 2 ? "gone" : "large";
                    Write(indexWriter.Index($"items/{i}"), $"items/{i}", status);
                    statuses[$"items/{i}"] = status;
                }

                indexWriter.Commit();
            }
        }

        AssertCounts(fields, statuses);

        using (var searcher = new IndexSearcher(Env, fields))
        {
            // every way a term keeps its entries is covered
            Assert.Equal("TermMatch [Set]", searcher.TermQuery("Status", "large").Inspect().Operation);
            Assert.Equal("TermMatch [SmallSet]", searcher.TermQuery("Status", "small").Inspect().Operation);
            Assert.Equal("TermMatch [Once]", searcher.TermQuery("Status", "single").Inspect().Operation);
            Assert.Equal("TermMatch [Empty]", searcher.TermQuery("Status", "no-such-status").Inspect().Operation);
        }

        using (var indexWriter = new IndexWriter(Env, fields, SupportedFeatures.All))
        {
            // move entries between terms, from a large posting list into a small one and back
            foreach (var id in statuses.Keys.Where((_, i) => i % 7 == 3).ToList())
            {
                var status = statuses[id] == "large" ? "small" : "large";
                Write(indexWriter.Update(System.Text.Encoding.UTF8.GetBytes(id)), id, status);
                statuses[id] = status;
            }

            // delete entries from every term, and every entry of "gone"
            foreach (var id in statuses.Keys.Where((key, i) => i % 11 == 5 || statuses[key] == "gone").ToList())
            {
                Assert.True(indexWriter.TryDeleteEntry(id));
                statuses.Remove(id);
            }

            indexWriter.Commit();
        }

        AssertCounts(fields, statuses);
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void EveryOtherMatch_IsRefused()
    {
        var fields = CreateFields(Allocator);
        using (var indexWriter = new IndexWriter(Env, fields, SupportedFeatures.All))
        {
            for (var i = 0; i < 500; i++)
                Write(indexWriter.Index($"items/{i}"), $"items/{i}", i % 3 == 0 ? "active" : "expired");

            indexWriter.Commit();
        }

        using var searcher = new IndexSearcher(Env, fields);
        var active = searcher.TermQuery("Status", "active");
        var expired = searcher.TermQuery("Status", "expired");

        var refused = new (string Name, IQueryMatch Match)[]
        {
            ("and", searcher.And(active, expired)),
            ("or", searcher.Or(active, expired)),
            ("and not", searcher.AndNot(searcher.AllEntries(), active)),
            ("starts with", searcher.StartWithQuery("Status", "act")),
            ("memoized term", searcher.Memoize(active).Replay()),
        };

        foreach (var (name, match) in refused)
            Assert.False(CoraxIndexReadOperation.TryGetExactMatchCount(match, out _), name);

        // the all-entries match itself is not what a query without a where clause produces, the memoized one is
        Assert.False(CoraxIndexReadOperation.TryGetExactMatchCount(searcher.AllEntries(), out _));
    }

    private void AssertCounts(IndexFieldsMapping fields, Dictionary<string, string> statuses)
    {
        using var searcher = new IndexSearcher(Env, fields);

        foreach (var status in new[] { "large", "small", "single", "gone", "no-such-status" })
        {
            var expected = statuses.Values.Count(x => x == status);

            Assert.True(CoraxIndexReadOperation.TryGetExactMatchCount(searcher.TermQuery("Status", status), out var count), status);
            Assert.Equal(expected, count);
            Assert.Equal(expected, Read(searcher.TermQuery("Status", status)));
        }

        Assert.True(CoraxIndexReadOperation.TryGetExactMatchCount(searcher.Memoize(searcher.AllEntries()).Replay(), out var all));
        Assert.Equal(statuses.Count, all);
        Assert.Equal(statuses.Count, Read(searcher.Memoize(searcher.AllEntries()).Replay()));
    }

    private static long Read(IQueryMatch match)
    {
        Span<long> ids = new long[4096];
        long total = 0;
        int read;
        while ((read = match.Fill(ids)) != 0)
            total += read;

        return total;
    }

    private static void Write(IndexWriter.IndexEntryBuilder builder, string id, string status)
    {
        using (builder)
        {
            builder.Write(IdField, null, id);
            builder.Write(StatusField, null, status);
            builder.EndWriting();
        }
    }

    private static IndexFieldsMapping CreateFields(ByteStringContext ctx)
    {
        Slice.From(ctx, "Id", ByteStringType.Immutable, out Slice idSlice);
        Slice.From(ctx, "Status", ByteStringType.Immutable, out Slice statusSlice);

        using (var builder = IndexFieldsMappingBuilder.CreateForWriter(false)
                   .AddBinding(IdField, idSlice)
                   .AddBinding(StatusField, statusSlice))
            return builder.Build();
    }
}
