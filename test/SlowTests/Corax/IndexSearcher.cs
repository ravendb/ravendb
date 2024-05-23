using System;
using System.Collections.Generic;
using System.Text;
using Corax;
using Corax.Analyzers;
using Corax.Mappings;
using FastTests.Voron;
using Sparrow;
using Sparrow.Server;
using Sparrow.Threading;
using Voron;
using Xunit;
using Xunit.Abstractions;
using IndexSearcher = Corax.Querying.IndexSearcher;
using IndexWriter = Corax.Indexing.IndexWriter;
using Tests.Infrastructure;

namespace SlowTests.Corax;

public class IndexSearcherTest : StorageTest
{
    public IndexSearcherTest(ITestOutputHelper output) : base(output)
    {
    }

    [RavenMultiplatformFact(RavenTestCategory.Corax, RavenArchitecture.X64)]
    public void SetTerm()
    {
        var entries = new IndexEntry[100000];
        var content = new string[] {"road"};

        for (int i = 0; i < entries.Length; i++)
        {
            entries[i] = new IndexEntry {Id = $"entry/{i}", Content = content,};
        }

        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        IndexEntries(bsc, entries, CreateKnownFields(bsc));

        {
            using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));
            var match = searcher.TermQuery("Content", "road");

            Assert.Equal(100000, match.Count);

            Span<long> ids = stackalloc long[1000];
            ids.Fill(-1);

            int read;
            int total = 0;
            do
            {
                read = match.Fill(ids);
                if (read != 0)
                {
                    Assert.Equal(1000, read);
                    Assert.False(ids.Contains(-1));
                    Assert.True(total <= match.Count);

                    ids.Fill(-1);
                }

                total += read;
            } while (read != 0);

            Assert.Equal(match.Count, total);
            Assert.Equal(0, match.Fill(ids));
        }
    }
    
    private class IndexEntry
    {
        public string Id;
        public string[] Content;
    }

    private readonly struct StringArrayIterator : IReadOnlySpanIndexer
    {
        private readonly string[] _values;

        private static string[] Empty = new string[0];

        public StringArrayIterator(string[] values)
        {
            _values = values ?? Empty;
        }
        
        public int Length => _values.Length;

        public bool IsNull(int i)
        {
            if (i < 0 || i >= Length)
                throw new ArgumentOutOfRangeException();

            return _values[i] == null;
        }

        public ReadOnlySpan<byte> this[int i] => _values[i] != null ? Encoding.UTF8.GetBytes(_values[i]) : null;
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

    private void IndexEntries(ByteStringContext bsc, IEnumerable<IndexEntry> list, IndexFieldsMapping mapping)
    {
        using var indexWriter = new IndexWriter(Env, mapping, SupportedFeatures.All);

        foreach (var entry in list)
        {
            CreateEntry(indexWriter, entry);
        }

        indexWriter.Commit();
        mapping.Dispose();
        
      
    }

    private static void CreateEntry(IndexWriter indexWriter, IndexEntry entry)
    {
        using var builder = indexWriter.Index(entry.Id);
        builder.Write(IdIndex, PrepareString(entry.Id));
        var it = new StringArrayIterator(entry.Content);
        for (int i = 0; i < it.Length; i++)
        {
            if (it.IsNull(i))
            {
                builder.WriteNull(ContentIndex, null);
            }
            else
            {
                builder.Write(ContentIndex, it[i]);
            }
        }
        builder.EndWriting();
        Span<byte> PrepareString(string value)
        {
            if (value == null)
                return Span<byte>.Empty;
            return Encoding.UTF8.GetBytes(value);
        }
    }
}
