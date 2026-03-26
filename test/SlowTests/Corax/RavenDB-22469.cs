using System;
using System.Collections.Generic;
using Corax;
using Corax.Indexing;
using Corax.Mappings;
using Corax.Querying;
using FastTests.Voron;
using Sparrow;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Corax;

public class RavenDB_22469(ITestOutputHelper output) : StorageTest(output)
{
    private const int e = 65_537;

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [InlineData(true)]
    [InlineData(false)]
    public void CanDeduplicateInPrimitive(bool useHashset)
    {
        List<long> entriesIds = new();
        using var mapping = IndexFieldsMappingBuilder.CreateForWriter(false)
            .AddBinding(0, "id()")
            .AddBinding(1, "Name")
            .Build();
        using (var indexWriter = new IndexWriter(Env, mapping, SupportedFeatures.All))
        {
            for (int i = 0; i < e; ++i)
            {
                using var builder = indexWriter.Index($"id/{i}");
                builder.Write(0, Encodings.Utf8.GetBytes($"id/{i}"));
                builder.IncrementList();
                builder.Write(1, Encodings.Utf8.GetBytes($"_{i}"));
                builder.Write(1, Encodings.Utf8.GetBytes($"_{e - i}"));
                builder.DecrementList();
                builder.EndWriting();
                entriesIds.Add((long)builder.EntryId);
            }
            indexWriter.Commit();
        }

        using (var indexSearcher = new IndexSearcher(Env, mapping))
        {
            var localResults = new List<long>();
            Span<long> ids = new long[16];

            var match = indexSearcher.DeduplicationMatch(indexSearcher.StartWithQuery("Name", "_"), forceHashset: useHashset);

            while (match.Fill(ids) is var read and > 0)
            {
                localResults.AddRange(ids[..read]);       
            }

            Assert.Equal(entriesIds.Count, localResults.Count);
            entriesIds.Sort();
            localResults.Sort();
            for (int i = 0; i < entriesIds.Count; ++i)
            {
                Assert.Equal(entriesIds[i], localResults[i]);
            }
        }
    }
}
