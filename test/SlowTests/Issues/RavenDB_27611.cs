using System;
using Corax;
using Corax.Indexing;
using Corax.Mappings;
using Corax.Querying;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using FastTests.Voron;
using Sparrow;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_27611 : StorageTest
{
    private const int NumberOfEntries = 100_000;
    private const long MaxManagedAllocationInBytes = 1024 * 1024;

    public RavenDB_27611(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void BitmapMatchesDoNotDrainChildrenThroughSmallCallerBuffer()
    {
        using IndexFieldsMapping mapping = IndexFieldsMappingBuilder.CreateForWriter(false)
            .AddBinding(0, "id()")
            .AddBinding(1, "Yard")
            .AddBinding(2, "Status")
            .Build();

        using (IndexWriter writer = new IndexWriter(Env, mapping, SupportedFeatures.All))
        {
            for (int i = 0; i < NumberOfEntries; i++)
            {
                using IndexWriter.IndexEntryBuilder builder = writer.Index($"tickets/{i}");
                builder.Write(0, Encodings.Utf8.GetBytes($"tickets/{i}"));
                builder.Write(1, Encodings.Utf8.GetBytes(i % 3 == 0 ? "YBHM" : "OTHER"));
                builder.Write(2, Encodings.Utf8.GetBytes(i % 2 == 0 ? "ACTIVE" : "PAID"));
                builder.EndWriting();
            }

            writer.Commit();
        }

        using IndexSearcher searcher = new IndexSearcher(Env, mapping) { BitmapAndFillMode = BitmapAndFillMode.Force };

        BinaryMatch and = searcher.And(searcher.InQuery("Yard", ["YBHM"]), searcher.InQuery("Status", ["ACTIVE", "PAID"]));
        AssertFillWithSmallBuffer(ref and, expected: 33_334);

        BinaryMatch or = searcher.Or(searcher.InQuery("Yard", ["YBHM"]), searcher.InQuery("Status", ["ACTIVE"]));
        AssertFillWithSmallBuffer(ref or, expected: 66_667);

        AndNotMatch andNot = searcher.AndNot(searcher.InQuery("Yard", ["YBHM"]), searcher.InQuery("Status", ["PAID"]));
        AssertFillWithSmallBuffer(ref andNot, expected: 16_667);
    }

    private static void AssertFillWithSmallBuffer<TMatch>(ref TMatch match, int expected)
        where TMatch : IQueryMatch
    {
        Span<long> small = new long[16];
        Span<long> large = new long[4096];

        long before = GC.GetAllocatedBytesForCurrentThread();
        int total = match.Fill(small);
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        int read;
        while ((read = match.Fill(large)) > 0)
            total += read;

        Assert.Equal(expected, total);
        Assert.True(allocated < MaxManagedAllocationInBytes, $"The first Fill allocated {allocated:N0} bytes of managed memory.");
    }
}
