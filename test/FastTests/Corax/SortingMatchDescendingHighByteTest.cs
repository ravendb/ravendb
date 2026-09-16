using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Corax;
using Corax.Mappings;
using Corax.Querying;
using Corax.Querying.Matches.SortingMatches.Meta;
using Corax.Utils;
using FastTests.Voron;
using Sparrow.Server;
using Sparrow.Threading;
using Tests.Infrastructure;
using Voron;
using Xunit;
using IndexWriter = Corax.Indexing.IndexWriter;

namespace FastTests.Corax;

/// <summary>
/// Isolated test for descending string sort with high-byte UTF-8 values.
/// Reproduces the StreamingOptimization_DataTests failures without going through
/// the full RavenDB server/client stack.
/// </summary>
public class SortingMatchDescendingHighByteTest : StorageTest
{
    private const int IdField = 0;
    private const int NameField = 1;

    public SortingMatchDescendingHighByteTest(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void DescendingStringSortWithHighByteValues()
    {
        // Use distinct high-Unicode strings that are lexicographically different.
        // Bytes {255,255,255}, {255,255,254}, {255,255,253} all decode to the SAME
        // UTF-8 string (three U+FFFD replacement characters), so their sort order is
        // undefined. Instead, use valid high-codepoint characters that are distinct.
        var entries = new (string Id, string Name)[]
        {
            ("entry/1", "aaaaa"),
            ("entry/2", "aaaab"),
            ("entry/3", "abaaa"),
            ("entry/4", "abaab"),
            ("entry/5", "acaaa"),
            ("entry/6", "acaab"),
            ("entry/7", "aeaaa"),
            ("entry/8", "aeaab"),
            ("entry/9", "afaaa"),
            ("entry/10", "afaab"),
            ("entry/11", "agaaa"),
            ("entry/12", "\u00FF\u00FF\u00FF"), // ÿÿÿ - highest Latin-1 char
            ("entry/13", "\u00FF\u00FF\u00FE"), // ÿÿþ
            ("entry/14", "\u00FF\u00FF\u00FD"), // ÿÿý
            ("entry/15", "bbbbbbbbb"),
        };

        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        IndexData(bsc, entries);

        using var searcher = new IndexSearcher(Env, CreateMapping(bsc));
        var nameMetadata = searcher.FieldMetadataBuilder("Name", NameField);

        // Descending sort — the three high-byte entries should come first: 255-255-255, 255-255-254, 255-255-253
        var orderMetadata = new OrderMetadata(nameMetadata, ascending: false, MatchCompareFieldType.Sequence);
        var allEntries = searcher.ExistsQuery(nameMetadata);
        var sortedMatch = searcher.OrderBy(allEntries, orderMetadata, defaultNullsSortMode: NullsSortMode.NullsLargest);

        var resultIds = new List<string>();
        Span<long> ids = stackalloc long[64];
        int read;
        do
        {
            read = sortedMatch.Fill(ids);
            for (int i = 0; i < read; i++)
                resultIds.Add(searcher.TermsReaderFor(searcher.GetFirstIndexedFiledName()).GetTermFor(ids[i]));
        } while (read > 0);

        // Build expected order: all names sorted descending
        var expectedIds = entries
            .OrderByDescending(e => e.Name, StringComparer.Ordinal)
            .Select(e => e.Id)
            .ToList();

        Assert.Equal(expectedIds.Count, resultIds.Count);
        for (int i = 0; i < expectedIds.Count; i++)
        {
            Assert.True(expectedIds[i] == resultIds[i],
                $"Position {i}: expected {expectedIds[i]} (Name={entries.First(e => e.Id == expectedIds[i]).Name}) " +
                $"but got {resultIds[i]} (Name={entries.First(e => e.Id == resultIds[i]).Name})");
        }
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void AscendingStringSortWithHighByteValues()
    {
        var entries = new (string Id, string Name)[]
        {
            ("entry/1", "aaaaa"),
            ("entry/2", "aaaab"),
            ("entry/3", "abaaa"),
            ("entry/12", "\u00FF\u00FF\u00FF"), // ÿÿÿ
            ("entry/13", "\u00FF\u00FF\u00FE"), // ÿÿþ
            ("entry/14", "\u00FF\u00FF\u00FD"), // ÿÿý
            ("entry/15", "bbbbbbbbb"),
        };

        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        IndexData(bsc, entries);

        using var searcher = new IndexSearcher(Env, CreateMapping(bsc));
        var nameMetadata = searcher.FieldMetadataBuilder("Name", NameField);

        // Ascending sort — high-byte entries should come last
        var orderMetadata = new OrderMetadata(nameMetadata, ascending: true, MatchCompareFieldType.Sequence);
        var allEntries = searcher.ExistsQuery(nameMetadata);
        var sortedMatch = searcher.OrderBy(allEntries, orderMetadata, defaultNullsSortMode: NullsSortMode.NullsLargest);

        var resultIds = new List<string>();
        Span<long> ids = stackalloc long[64];
        int read;
        do
        {
            read = sortedMatch.Fill(ids);
            for (int i = 0; i < read; i++)
                resultIds.Add(searcher.TermsReaderFor(searcher.GetFirstIndexedFiledName()).GetTermFor(ids[i]));
        } while (read > 0);

        var expectedIds = entries
            .OrderBy(e => e.Name, StringComparer.Ordinal)
            .Select(e => e.Id)
            .ToList();

        Assert.Equal(expectedIds.Count, resultIds.Count);
        for (int i = 0; i < expectedIds.Count; i++)
        {
            Assert.True(expectedIds[i] == resultIds[i],
                $"Position {i}: expected {expectedIds[i]} (Name={entries.First(e => e.Id == expectedIds[i]).Name}) " +
                $"but got {resultIds[i]} (Name={entries.First(e => e.Id == resultIds[i]).Name})");
        }
    }

    private void IndexData(ByteStringContext bsc, (string Id, string Name)[] entries)
    {
        var mapping = CreateMapping(bsc);
        using var indexWriter = new IndexWriter(Env, mapping, SupportedFeatures.All);
        foreach (var (id, name) in entries)
        {
            using var builder = indexWriter.Index(id);
            builder.Write(IdField, Encoding.UTF8.GetBytes(id));
            builder.Write(NameField, Encoding.UTF8.GetBytes(name));
            builder.EndWriting();
        }
        indexWriter.Commit();
        mapping.Dispose();
    }

    private static IndexFieldsMapping CreateMapping(ByteStringContext ctx)
    {
        Slice.From(ctx, "Id", ByteStringType.Immutable, out Slice idSlice);
        Slice.From(ctx, "Name", ByteStringType.Immutable, out Slice nameSlice);
        using var builder = IndexFieldsMappingBuilder.CreateForWriter(false)
            .AddBinding(IdField, idSlice)
            .AddBinding(NameField, nameSlice);
        return builder.Build();
    }
}
