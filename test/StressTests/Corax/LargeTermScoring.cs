using System;
using System.Linq;
using System.Text;
using Corax;
using Corax.Indexing;
using Corax.Mappings;
using Corax.Querying;
using FastTests.Voron;
using Tests.Infrastructure;
using Xunit;

namespace StressTests.Corax;

public class LargeTermScoring(ITestOutputHelper output) : StorageTest(output)
{
    [RavenFact(RavenTestCategory.Corax)]
    public void EveryDocumentOfTermWithLargePostingListIsScored()
    {
        const int numberOfEntries = 150_000;
        const int entriesWithTerm = 120_000;

        using var mapping = IndexFieldsMappingBuilder.CreateForWriter(false).AddBinding(0, "Id").AddBinding(1, "Status").Build();
        using (var writer = new IndexWriter(Env, mapping, SupportedFeatures.All))
        {
            for (var i = 0; i < numberOfEntries; i++)
            {
                using var builder = writer.Index($"entries/{i}");
                builder.Write(0, Encoding.UTF8.GetBytes($"entries/{i}"));
                if (i % 5 == 0)
                {
                    builder.Write(1, "inactive"u8);
                }
                else
                {
                    // Varying term frequency keeps the posting list from compressing into a single page.
                    for (var occurrence = 0; occurrence <= i % 4; occurrence++)
                        builder.Write(1, "active"u8);
                }

                builder.EndWriting();
            }

            writer.Commit();
        }

        using var searcher = new IndexSearcher(Env, mapping);
        var match = searcher.TermQuery(searcher.FieldMetadataBuilder("Status", 1, hasBoost: true), "active");

        var ids = new long[numberOfEntries];
        var read = 0;
        while (match.Fill(ids.AsSpan(read)) is var count and > 0)
            read += count;

        Assert.Equal(entriesWithTerm, read);

        var scores = new float[read];
        match.Score(ids.AsSpan(0, read), scores, 1f);

        Assert.Equal(entriesWithTerm, scores.Count(score => score > 0));
    }
}
