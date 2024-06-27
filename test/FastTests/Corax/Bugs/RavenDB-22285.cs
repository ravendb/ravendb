using System;
using System.Collections.Generic;
using System.Linq;
using Corax;
using Corax.Analyzers;
using Corax.Indexing;
using Corax.Mappings;
using Corax.Querying;
using FastTests.Voron;
using Sparrow;
using Sparrow.Server;
using Sparrow.Threading;
using Tests.Infrastructure;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax.Bugs;

public class RavenDB_22285 : StorageTest
{
    private const int Id = 0, Content = 1, Content2 = 2;

    private record Dto(string Id, string Content, string Content2, int Position);
    
    [RavenFact(RavenTestCategory.Corax)]
    public void MultiTermMatchReturnsGoodResults()
    {
        using var allocator = new ByteStringContext(SharedMultipleUseFlag.None);
        using var mapping = CreateKnownFields(allocator);
        var descendingSortedTerm = Enumerable.Range(0, 26).Select(i => new[] { (byte)('z' - i) }).ToList();
        var database = new List<Dto>();
        using (var indexWriter = new IndexWriter(Env, mapping, SupportedFeatures.All))
        {
            for (var idX = 0; idX < 2096; ++idX)
            {
                var id = Encodings.Utf8.GetBytes($"item/{idX}");
                using var result = indexWriter.Index(id);
                result.Write(0, id);
                result.Write(1, descendingSortedTerm[idX % descendingSortedTerm.Count]);
                result.Write(2, Encodings.Utf8.GetBytes($"common{(idX % 10).ToString()}"));
                database.Add(new Dto($"item/{idX}", Encodings.Utf8.GetString(descendingSortedTerm[idX % descendingSortedTerm.Count]), $"common{idX % 10}", (int)result.EntryId));
            }
            
            indexWriter.Commit();
        }

        using (var indexSearcher = new IndexSearcher(Env, mapping))
        {
            var termsReader =  indexSearcher.TermsReaderFor(nameof(Id));
            var exists = indexSearcher.ExistsQuery(mapping.GetByFieldId(Content).Metadata);
            var termMatch = indexSearcher.TermQuery(mapping.GetByFieldId(Content2).Metadata, "common0");
            var buffer = new long[512];
            var results = termMatch.Fill(buffer);
            results = exists.AndWith(buffer, results);
            AssertSorted(buffer, results);

            var inMemoryResult = database.Where(x => x.Content2 == "common0").OrderBy(x => x.Position).ToList();
            Assert.Equal(inMemoryResult.Count, results);

            for (int idX = 0; idX < results; ++idX)
            {
                Assert.True(termsReader.TryGetTermFor(buffer[idX], out var documentId));
                Assert.Equal(inMemoryResult[idX].Id, documentId);
            }
        }

    }

    private static void AssertSorted(Span<long> buffer, int results)
    {
        for (var i = 1; i < results; ++i)
            Assert.True(buffer[i - 1] <= buffer[i], $"buffer[{i-1}] <= buffer[{i}] ({buffer[i-1]} <= {buffer[i]})");
    }


    private static IndexFieldsMapping CreateKnownFields(ByteStringContext ctx, Analyzer analyzer = null)
    {
        Slice.From(ctx, nameof(Id), ByteStringType.Immutable, out Slice idSlice);
        Slice.From(ctx, nameof(Content), ByteStringType.Immutable, out Slice contentSlice);
        Slice.From(ctx, nameof(Content2), ByteStringType.Immutable, out Slice content2Slice);

        using var builder = IndexFieldsMappingBuilder.CreateForWriter(false)
            .AddBinding(0, idSlice, analyzer)
            .AddBinding(1, contentSlice, analyzer)
            .AddBinding(2, content2Slice, analyzer);
        return builder.Build();
    }
    
    public RavenDB_22285(ITestOutputHelper output) : base(output)
    {
    }
}
