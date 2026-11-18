using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Corax;
using Corax.Analyzers;
using Corax.Mappings;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Primitives;
using FastTests.Voron;
using Sparrow.Server;
using Sparrow.Threading;
using Tests.Infrastructure;
using Voron;
using Voron.Data.RoaringBitmaps;
using Xunit;
using IndexSearcher = Corax.Querying.IndexSearcher;
using IndexWriter = Corax.Indexing.IndexWriter;


namespace StressTests.Corax;

public class IndexSearcherTest : StorageTest
{
    private class IndexSingleEntry
    {
        public string Id;
        public string Content;
    }

    public const int IdIndex = 0,
        ContentIndex = 1;

    private static IndexFieldsMapping CreateKnownFields(ByteStringContext ctx, Analyzer analyzer = null)
    {
        Slice.From(ctx, "Id", ByteStringType.Immutable, out Slice idSlice);
        Slice.From(ctx, "Content", ByteStringType.Immutable, out Slice contentSlice);

        using var builder = IndexFieldsMappingBuilder.CreateForWriter(false)
            .AddBinding(IdIndex, idSlice, analyzer)
            .AddBinding(ContentIndex, contentSlice, analyzer);
        return builder.Build();
    }

    private void IndexEntries(ByteStringContext bsc, IEnumerable<IndexSingleEntry> list, IndexFieldsMapping mapping)
    {
        using var indexWriter = new IndexWriter(Env, mapping, SupportedFeatures.All);

        foreach (var entry in list)
        {
            CreateEntry(indexWriter, entry);
        }

        indexWriter.Commit();
        mapping.Dispose();
    }

  
    private static void CreateEntry(IndexWriter indexWriter, IndexSingleEntry entry)
    {
        using var builder = indexWriter.Index(entry.Id);
        builder.Write(IdIndex, PrepareString(entry.Id));
        builder.Write(ContentIndex, PrepareString(entry.Content));
        builder.EndWriting();
        Span<byte> PrepareString(string value)
        {
            if (value == null)
                return Span<byte>.Empty;
            return Encoding.UTF8.GetBytes(value);
        }
    }
    
    public IndexSearcherTest(ITestOutputHelper output) : base(output)
    {
    }

    // IndexSearcher has no And/AndNot — build equivalents via bitmap primitives.
    private static BitmapMatch AndNot(IndexSearcher searcher, IQueryMatch left, IQueryMatch right)
    {
        var bitmap = new BitmapMatch(searcher.Allocator);
        RoaringBitmap tempData = new(searcher.Allocator);
        try
        {
            QueryPrimitives.OrWithMatch(left, ref bitmap.BitmapState);
            QueryPrimitives.AndNotWithMatch(right, ref bitmap.BitmapState, ref tempData);
        }
        finally
        {
            tempData.Dispose();
        }
        return bitmap;
    }

    private static BitmapMatch And(IndexSearcher searcher, IQueryMatch left, IQueryMatch right)
    {
        var bitmap = new BitmapMatch(searcher.Allocator);
        RoaringBitmap tempData = new(searcher.Allocator);
        try
        {
            QueryPrimitives.OrWithMatch(left, ref bitmap.BitmapState);
            QueryPrimitives.AndWithMatch(right, ref bitmap.BitmapState, ref tempData);
        }
        finally
        {
            tempData.Dispose();
        }
        return bitmap;
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void BigAndNotMemoized()
    {
        int total = 100000;

        int startWith = 0;
        var entries = new List<IndexSingleEntry>();
        for (int i = 0; i < total; i++)
        {
            var content = i.ToString("000000");
            entries.Add(new IndexSingleEntry {Id = $"entry/{content}", Content = content});
            if (content.StartsWith("00"))
                startWith++;
        }


        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        IndexEntries(bsc, entries, CreateKnownFields(bsc));

        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        Slice.From(ctx, "Id", ByteStringType.Immutable, out Slice idSlice);
        Slice.From(ctx, "Content", ByteStringType.Immutable, out Slice contentSlice);

        using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));

        // No Memoize available; AllEntries() is cheap (just iterates the entry index), so call it
        // fresh each time a multi-consumer expression needs a new source.

        {
            var andNotMatch = AndNot(searcher,searcher.AllEntries(), searcher.AllEntries());

            Span<long> ids = stackalloc long[4096];

            int counter = 0;
            int read;
            do
            {
                read = andNotMatch.Fill(ids);
                counter += read;
            } while (read != 0);

            Assert.Equal(0, counter);
        }

        {
            var andNotMatch = AndNot(searcher,searcher.AllEntries(), searcher.StartWithQuery("Content", "J"));

            Span<long> ids = stackalloc long[4096];
            int counter = 0;
            int read;
            do
            {
                read = andNotMatch.Fill(ids);
                counter += read;
            } while (read != 0);

            Assert.Equal(entries.Count, counter);
        }

        {
            var andNotMatch = AndNot(searcher,searcher.AllEntries(), searcher.StartWithQuery("Content", "00"));

            Span<long> ids = stackalloc long[4096];

            var entriesLookup = new HashSet<string>();
            int read;
            do
            {
                read = andNotMatch.Fill(ids);

                for (int i = 0; i < read; i++)
                {
                    long id1 = ids[i];
                    var id = searcher.TermsReaderFor(searcher.GetFirstIndexedFiledName()).GetTermFor(id1);
                    Assert.False(id.StartsWith("entry/00"));
                    entriesLookup.Add(id);
                }
            } while (read != 0);

            Assert.Equal(total - startWith, entriesLookup.Count);
        }

        {
            // And(AllEntries(), AndNot(AllEntries(), StartWith("00"))) — tests AND composition
            // on top of ANDNOT. The outer AND should not change the result.
            using var andNotResult = AndNot(searcher, searcher.AllEntries(), searcher.StartWithQuery("Content", "00"));
            using var andMatch = And(searcher, searcher.AllEntries(), andNotResult);

            Span<long> ids = stackalloc long[4096];

            var entriesLookup = new HashSet<string>();
            int read;
            do
            {
                read = andMatch.Fill(ids);

                for (int i = 0; i < read; i++)
                {
                    long id1 = ids[i];
                    var id = searcher.TermsReaderFor(searcher.GetFirstIndexedFiledName()).GetTermFor(id1);
                    Assert.False(id.StartsWith("entry/00"));
                    entriesLookup.Add(id);
                }
            } while (read != 0);

            Assert.Equal(total - startWith, entriesLookup.Count);
        }
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void BigAndNot()
    {
        int total = 100_000;

        int startWith = 0;
        var entries = new List<IndexSingleEntry>();
        for (int i = 0; i < total; i++)
        {
            var content = i.ToString("000000");
            entries.Add(new IndexSingleEntry {Id = $"entry/{content}", Content = content});
            if (content.StartsWith("00"))
                startWith++;
        }


        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        IndexEntries(bsc, entries, CreateKnownFields(bsc));

        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        Slice.From(ctx, "Id", ByteStringType.Immutable, out Slice idSlice);
        Slice.From(ctx, "Content", ByteStringType.Immutable, out Slice contentSlice);

        using var searcher = new IndexSearcher(base.Env, CreateKnownFields(bsc));

        {
            var andNotMatch = AndNot(searcher,searcher.AllEntries(), searcher.AllEntries());

            Span<long> ids = stackalloc long[4096];

            int counter = 0;
            int read;
            do
            {
                read = andNotMatch.Fill(ids);
                counter += read;
            } while (read != 0);

            Assert.Equal(0, counter);
        }

        {
            var andNotMatch = AndNot(searcher,searcher.AllEntries(), searcher.StartWithQuery("Content", "J"));

            Span<long> ids = stackalloc long[4096];
            int counter = 0;
            int read;
            do
            {
                read = andNotMatch.Fill(ids);
                counter += read;
            } while (read != 0);

            Assert.Equal(entries.Count, counter);
        }

        {
            var andNotMatch = AndNot(searcher,searcher.AllEntries(), searcher.StartWithQuery("Content", "00"));

            Span<long> ids = stackalloc long[4096];

            var entriesLookup = new HashSet<string>();
            int read;
            do
            {
                read = andNotMatch.Fill(ids);

                for (int i = 0; i < read; i++)
                {
                    long id1 = ids[i];
                    var id = searcher.TermsReaderFor(searcher.GetFirstIndexedFiledName()).GetTermFor(id1);
                    Assert.False(id.StartsWith("entry/00"));
                    entriesLookup.Add(id);
                }
            } while (read != 0);

            Assert.Equal(total - startWith, entriesLookup.Count);
        }

        {
            // And(AllEntries(), AndNot(AllEntries(), StartWith("00"))) — tests AND composition
            // on top of ANDNOT. The outer AND should not change the result.
            using var andNotResult = AndNot(searcher, searcher.AllEntries(), searcher.StartWithQuery("Content", "00"));
            using var andMatch = And(searcher, searcher.AllEntries(), andNotResult);

            Span<long> ids = stackalloc long[4096];

            var entriesLookup = new HashSet<string>();
            int read;
            do
            {
                read = andMatch.Fill(ids);

                for (int i = 0; i < read; i++)
                {
                    long id1 = ids[i];
                    var id = searcher.TermsReaderFor(searcher.GetFirstIndexedFiledName()).GetTermFor(id1);
                    Assert.False(id.StartsWith("entry/00"));
                    entriesLookup.Add(id);
                }
            } while (read != 0);

            Assert.Equal(total - startWith, entriesLookup.Count);
        }
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void BigContainsTest()
    {
        var ids = ArrayPool<long>.Shared.Rent(2048);
        var random = new Random(1000);

        var strings = new string[]
        {
            "ing", "hehe", "sad", "mac", "iej", "asz", "yk", "rav", "endb", "co", "rax", "mix", "ture", "net", "fram", "work", "th", "is", " ", "gre", "at", "te",
            "st"
        };

        string GetRandomText()
        {
            var l = strings.Length;
            int l_new_word = random.Next(l);
            if (l_new_word is 0)
                l_new_word = 1;
            var sb = new StringBuilder();
            for (int i = 0; i < l_new_word; i++)
            {
                sb.Append(strings[random.Next(0, l)]);
            }

            return sb.ToString();
        }

        try
        {
            using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
            var list = Enumerable.Range(0, 128_000).Select(x => new IndexSingleEntry() {Id = $"entry/{x}", Content = GetRandomText()}).ToList();

            Slice.From(ctx, "Id", ByteStringType.Immutable, out Slice idSlice);
            Slice.From(ctx, "Content", ByteStringType.Immutable, out Slice contentSlice);
            using var builder = IndexFieldsMappingBuilder.CreateForWriter(false)
                .AddBinding(IdIndex, idSlice)
                .AddBinding(ContentIndex, contentSlice);
            using var mapping = builder.Build();

            IndexEntries(ctx, list, mapping);

            using var searcher = new IndexSearcher(Env, mapping);
            {
                var match = searcher.ContainsQuery(searcher.FieldMetadataBuilder("Content"), "ing");
                int read;
                int whole = 0;
                var reader = searcher.TermsReaderFor(contentSlice);
                while ((read = match.Fill(ids)) != 0)
                {
                    whole += read;
                    foreach (var id in ids)
                    {
                        string term = reader.GetTermFor(id);
                        Assert.True(term.Contains("ing"));
                    }
                }

                Assert.Equal(list.Count(x => x.Content.Contains("ing")), whole);
            }
        }
        catch
        {
            throw;
        }
        finally
        {
            ArrayPool<long>.Shared.Return(ids);
        }
    }

}
