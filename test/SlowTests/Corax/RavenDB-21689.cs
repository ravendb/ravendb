using System;
using System.Linq;
using System.Threading.Tasks;
using Corax;
using Corax.Indexing;
using Corax.Mappings;
using Corax.Querying;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Primitives;
using Corax.Utils;
using FastTests.Voron;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers;
using Sparrow;
using Sparrow.Server;
using Sparrow.Threading;
using Tests.Infrastructure;
using Voron.Data.RoaringBitmaps;
using Xunit;

namespace SlowTests.Corax;

public class RavenDB_21689 : StorageTest
{
    private readonly ByteStringContext _bsc;
    private readonly IndexFieldsMapping _fieldsMapping;

    public RavenDB_21689(ITestOutputHelper output) : base(output)
    {
        _bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        _fieldsMapping = IndexFieldsMappingBuilder
            .CreateForWriter(false)
            .AddBinding(0, "id")
            .AddBinding(1, "boolean")
            .AddBinding(2, "text", LuceneAnalyzerAdapter.Create(new RavenStandardAnalyzer(Lucene.Net.Util.Version.LUCENE_30), forQuerying: false))
            .Build();
    }

    // Regression for RavenDB-21689: intersecting a large (Set-backed) TermMatch posting list against a
    // candidate set must terminate and produce the correct intersection. The original bug was an infinite
    // loop in TermMatch's own AndWith merge; that path was removed and the production intersection now runs
    // through QueryPrimitives.AndWithMatch -> AndWithPostings (the bounded posting-list range scan). This
    // test exercises that path: half the docs match the term ("false"), so the term has a large posting
    // list, and the candidate set (a wildcard search) overlaps it partially, making the intersection a
    // genuine subset rather than the whole candidate set.
    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void LargeSetTermMatchAndWithPostingsIntersectsCorrectlyAndTerminates()
    {
        const int docsSize = 64 * 1000;
        using var defaultAnalyzer = global::Corax.Analyzers.Analyzer.CreateLowercaseAnalyzer(_bsc);
        using (var indexWriter = new IndexWriter(Env, _fieldsMapping, SupportedFeatures.All))
        {
            for (var docIdx = 0; docIdx < docsSize; ++docIdx)
            {
                using var builder = indexWriter.Index($"doc/{docIdx}");
                builder.Write(0, Encodings.Utf8.GetBytes($"doc/{docIdx}"));
                // Half "false" / half "true": "false" still yields a 32k-entry posting list (Set-backed
                // TermMatch -> AndWithPostings), but is now a real filter rather than matching every doc.
                builder.Write(1, Encodings.Utf8.GetBytes(docIdx % 2 == 0 ? "true" : "false"));
                builder.Write(2, Encodings.Utf8.GetBytes($"abc{docIdx}"));
                builder.EndWriting();
            }

            indexWriter.Commit();
        }

        using (var indexSearcher = new IndexSearcher(Env, _fieldsMapping))
        {
            // Candidate set: a wildcard search spread across the id space (abc10, abc100-109, abc1000-1099, ...).
            var searchQuery = indexSearcher.SearchQuery(
                _fieldsMapping.GetByFieldId(2).Metadata.ChangeAnalyzer(FieldIndexingMode.Search, defaultAnalyzer),
                new[] { "abc10*" }, Constants.Search.Operator.Or);
            var searchIds = Drain(searchQuery, docsSize);
            Array.Sort(searchIds);

            // The large posting-list term we will intersect with.
            var termIds = Drain(indexSearcher.TermQuery(_fieldsMapping.GetByFieldId(1).Metadata, "false"), docsSize);
            Array.Sort(termIds);

            // Expected intersection computed inline (both inputs sorted ascending).
            var expected = Intersect(searchIds, termIds);

            // Production path: AndWithMatch routes a Set-backed TermMatch into the bounded AndWithPostings scan,
            // intersecting the candidate bitmap in place.
            var bitmap = new RoaringBitmap(indexSearcher.Allocator);
            var tempBitmap = new RoaringBitmap(indexSearcher.Allocator);
            try
            {
                bitmap.AddRange(searchIds);

                var termQuery = indexSearcher.TermQuery(_fieldsMapping.GetByFieldId(1).Metadata, "false");
                QueryPrimitives.AndWithMatch(termQuery, ref bitmap, ref tempBitmap);

                var actual = ReadAll(ref bitmap);
                Array.Sort(actual);

                Assert.True(expected.Length > 0 && expected.Length < searchIds.Length,
                    $"expected a partial intersection (got {expected.Length} of {searchIds.Length} candidates)");
                Assert.Equal(expected, actual);
            }
            finally
            {
                bitmap.Dispose();
                tempBitmap.Dispose();
            }
        }
    }

    private static long[] Drain(IQueryMatch match, int capacity)
    {
        var ids = new long[capacity];
        Span<long> fillBuffer = ids;
        var results = 0;
        while (match.Fill(fillBuffer) is var read and > 0)
        {
            fillBuffer = fillBuffer.Slice(read);
            results += read;
        }

        return ids[..results];
    }

    private static long[] ReadAll(ref RoaringBitmap bitmap)
    {
        bitmap.PrepareForReading();
        var iterator = bitmap.GetIterator();
        var result = new System.Collections.Generic.List<long>();
        Span<long> buffer = stackalloc long[1024];
        int read;
        while ((read = iterator.Fill(ref bitmap, buffer)) > 0)
        {
            for (int i = 0; i < read; i++)
                result.Add(buffer[i]);
        }

        return result.ToArray();
    }

    private static long[] Intersect(long[] sortedA, long[] sortedB)
    {
        var result = new System.Collections.Generic.List<long>();
        int i = 0, j = 0;
        while (i < sortedA.Length && j < sortedB.Length)
        {
            if (sortedA[i] < sortedB[j]) i++;
            else if (sortedA[i] > sortedB[j]) j++;
            else { result.Add(sortedA[i]); i++; j++; }
        }

        return result.ToArray();
    }

    public override async ValueTask DisposeAsync()
    {
        _bsc?.Dispose();
        _fieldsMapping?.Dispose();
        await base.DisposeAsync();
    }
}
