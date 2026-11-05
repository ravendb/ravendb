using System;
using System.Text;
using Corax;
using Corax.Mappings;
using FastTests.Voron;
using Sparrow.Server;
using Tests.Infrastructure;
using Voron;
using Xunit;
using IndexSearcher = Corax.Querying.IndexSearcher;
using IndexWriter = Corax.Indexing.IndexWriter;

namespace FastTests.Corax;

public class IndexWriterDeletesSharedKey(ITestOutputHelper output) : StorageTest(output)
{
    private const int IdIndex = 0, ContentIndex = 1;

    private IndexFieldsMapping CreateMapping()
    {
        Slice.From(Allocator, "Id", ByteStringType.Immutable, out var idSlice);
        Slice.From(Allocator, "Content", ByteStringType.Immutable, out var contentSlice);
        using var builder = IndexFieldsMappingBuilder.CreateForWriter(false).AddBinding(IdIndex, idSlice).AddBinding(ContentIndex, contentSlice);
        return builder.Build();
    }

    private static void Add(IndexWriter writer, int i)
    {
        using var builder = writer.Index(Encoding.UTF8.GetBytes($"list/{i}"));
        builder.Write(IdIndex, null, Encoding.UTF8.GetBytes($"list/{i}"));
        builder.Write(ContentIndex, null, Encoding.UTF8.GetBytes((i % 10).ToString()), i % 10, i % 10);
        builder.EndWriting();
    }

    private void Populate(IndexFieldsMapping mapping, int count)
    {
        using var writer = new IndexWriter(Env, mapping, SupportedFeatures.All);
        for (var i = 0; i < count; i++)
            Add(writer, i);
        writer.Commit();
    }

    private long CountTerm(IndexFieldsMapping mapping, string field, string term)
    {
        using var searcher = new IndexSearcher(Env, mapping);
        var match = searcher.TermQuery(field, term);
        var buffer = new long[8192];
        var total = 0L;
        int read;
        while ((read = match.Fill(buffer)) > 0)
            total += read;
        return total;
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void ManyDeletesThroughOneWriterRemoveEveryEntry()
    {
        const int count = 3_000;
        using var mapping = CreateMapping();
        Populate(mapping, count);

        using (var writer = new IndexWriter(Env, mapping, SupportedFeatures.All))
        {
            for (var i = 0; i < count; i += 2)
                Assert.True(writer.TryDeleteEntry($"list/{i}"));
            writer.Commit();
        }

        for (var digit = 0; digit < 10; digit++)
            Assert.Equal(digit % 2 == 0 ? 0 : count / 10, CountTerm(mapping, "Content", digit.ToString()));

        Assert.Equal(0, CountTerm(mapping, "Id", "list/0"));
        Assert.Equal(1, CountTerm(mapping, "Id", "list/1"));
        Assert.Equal(0, CountTerm(mapping, "Id", "list/2998"));
        Assert.Equal(1, CountTerm(mapping, "Id", "list/2999"));
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void DeletingAndAddingTheSameEntriesThroughOneWriterIsAnUpdate()
    {
        const int count = 1_000;
        using var mapping = CreateMapping();
        Populate(mapping, count);

        using (var writer = new IndexWriter(Env, mapping, SupportedFeatures.All))
        {
            for (var i = 0; i < count; i++)
            {
                writer.TryDeleteEntry($"list/{i}");
                Add(writer, i);
            }

            writer.Commit();
        }

        Assert.Equal(count / 10, CountTerm(mapping, "Content", "3"));
        for (var i = 0; i < count; i += 111)
            Assert.Equal(1, CountTerm(mapping, "Id", $"list/{i}"));

        using var searcher = new IndexSearcher(Env, mapping);
        var all = new long[2048];
        Assert.Equal(count, searcher.AllEntries().Fill(all));
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void DeletingEntriesThatDoNotExistChangesNothing()
    {
        using var mapping = CreateMapping();
        Populate(mapping, 50);

        using (var writer = new IndexWriter(Env, mapping, SupportedFeatures.All))
        {
            Assert.False(writer.TryDeleteEntry("list/missing"));
            Assert.False(writer.TryDeleteEntry("list/also-missing"));
            writer.Commit();
        }

        Assert.Equal(5, CountTerm(mapping, "Content", "7"));
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void WriterThatDeletesNothingCanBeDisposedWithoutCommit()
    {
        using var mapping = CreateMapping();
        Populate(mapping, 10);

        var writer = new IndexWriter(Env, mapping, SupportedFeatures.All);
        writer.Dispose();

        Assert.Equal(1, CountTerm(mapping, "Content", "7"));
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void WriterThatDeletedEntriesCanBeDisposedWithoutCommit()
    {
        using var mapping = CreateMapping();
        Populate(mapping, 100);

        var writer = new IndexWriter(Env, mapping, SupportedFeatures.All);
        for (var i = 0; i < 50; i++)
            writer.TryDeleteEntry($"list/{i}");
        writer.Dispose();

        Assert.Equal(10, CountTerm(mapping, "Content", "1"));
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void DeletionsAllocateFarLessThanAKeyPerEntry()
    {
        const int count = 2_000;
        using var mapping = CreateMapping();
        Populate(mapping, count);

        // warm up on a first writer
        using (var warmup = new IndexWriter(Env, mapping, SupportedFeatures.All))
        {
            warmup.TryDeleteEntry("list/0");
            warmup.Commit();
        }

        long allocated;
        using (var writer = new IndexWriter(Env, mapping, SupportedFeatures.All))
        {
            var before = GC.GetAllocatedBytesForCurrentThread();
            for (var i = 1; i < count; i++)
                writer.TryDeleteEntry($"list/{i}");
            writer.Commit();
            allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        }

        var perEntry = allocated / (double)(count - 1);
        Output.WriteLine($"managed bytes per deleted entry: {perEntry:F0}");
        Assert.True(perEntry < 4_500, $"{perEntry:F0} bytes per deleted entry");
        Assert.Equal(0, CountTerm(mapping, "Content", "5"));
    }
}
