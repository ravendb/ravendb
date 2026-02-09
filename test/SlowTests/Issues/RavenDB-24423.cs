using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Corax.Pipeline;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Sparrow;
using Sparrow.Server;
using Sparrow.Threading;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTest.Corax;

public class RavenDB_24423(ITestOutputHelper output) : RavenTestBase(output)
{
    private record Dto(string Title, int? IntValue = null, string Id = null);

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void CanSortBengaliAlphabetByOrderByAlphaNumeric(Options options)
    {
        using var store = GetDocumentStore(options);
        using (var session = store.OpenSession())
        {
            session.Store(new Dto("বাংলাবর্ণমালাবালিপি"));
            session.Store(new Dto("বাংলাবর্ণমালা1"));
            session.SaveChanges();

            var result = session.Advanced.DocumentQuery<Dto>()
                .WaitForNonStaleResults()
                .OrderBy(x => x.Title, OrderingType.AlphaNumeric)
                .ToList();
            Assert.NotNull(result);
        }
    }


    [RavenTheory(RavenTestCategory.Corax)]
    [InlineData("𐀀")]
    [InlineData("𐀁")]
    public void Utf8_4BytesCanBeParsed(string str)
    {
        var bytes = Encoding.UTF8.GetBytes(str);
        Assert.Equal(4, bytes.Length);
        Token[] tokens = new Token[16];
        byte[] dest = new byte[64];
        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        using var defaultAnalyzer = global::Corax.Analyzers.Analyzer.CreateLowercaseAnalyzer(bsc);
        var outputTokens = tokens.AsSpan();
        var outputBytes = dest.AsSpan();
        defaultAnalyzer.Execute(bytes.AsSpan(), ref outputBytes, ref outputTokens);
        Assert.Equal(4, outputBytes.Length);

        Assert.Equal(1, outputTokens.Length);
        Assert.Equal(4, (int)outputTokens[0].Length);
    }


    [RavenTheory(RavenTestCategory.Corax)]
    [InlineData(1, 0x007F)]
    [InlineData(0x0080, 0x07FF)]
    [InlineData(0x0800, 0xFFFF)]
    [InlineData(0x010000, 0x10FFFF)]
    public void CanParseAllRunes(int from, int toInclusive)
    {
        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        using var defaultAnalyzer = global::Corax.Analyzers.Analyzer.CreateLowercaseAnalyzer(bsc);
        Span<Token> tokens = stackalloc Token[16];
        Span<byte> buffer = stackalloc byte[16];
        for (int i = from; i <= toInclusive; ++i)
        {
            if (Rune.TryCreate(i, out var rune) == false)
                continue;

            var localTokens = tokens;
            var localBuffer = buffer;

            var str = rune.ToString();
            var sourceString = Encodings.Utf8.GetBytes(str);

            defaultAnalyzer.Execute(sourceString.AsSpan(), ref localBuffer, ref localTokens);

            var lowerRune = Rune.ToLowerInvariant(rune);
            var lowerStr = lowerRune.ToString();
            var lowerBytes = Encodings.Utf8.GetBytes(lowerStr);
            Assert.Equal(lowerBytes.Length, (int)localTokens[0].Length);
            Assert.True(lowerBytes.AsSpan().SequenceEqual(localBuffer.Slice(0, lowerBytes.Length)));
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [InlineData(1, 0x007F)]
    [InlineData(0x0080, 0x07FF)]
    [InlineData(0x0800, 0xFFFF)]
    [InlineData(0x010000, 0x10FFFF)]
    public void Test(int from, int toInclusive)
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        var localDtos = new List<Dto>();
        for (int i = from; i <= toInclusive; ++i)
        {
            if (Rune.TryCreate(i, out var rune) == false)
                continue;

            var str = rune.ToString();
            localDtos.Add(new(str, IntValue: i));

            var bytes = Encodings.Utf8.GetBytes(str);
            var (byteLengthOfCharacter, charNeededToEncodeCharacters) = bytes[0] switch
            {
                <= 0b0111_1111 => (1, 1), /* 1 byte sequence: 0b0xxxxxxxx */
                <= 0b1101_1111 => (2, 1), /* 2 byte sequence: 0b110xxxxxx */
                <= 0b1110_1111 => (3, 1), /* 0b1110xxxx: 3 bytes sequence */
                <= 0b1111_0111 => (4, 2), /* 0b11110xxx: 4 bytes sequence */
                _ => throw new InvalidDataException($"Characters should be between 1 and 4 bytes long and cannot match the specified sequence. This is invalid code.")
            };

            Assert.Equal(bytes.Length, byteLengthOfCharacter);
            Assert.Equal(str.AsSpan().Length, charNeededToEncodeCharacters);
        }

        using (var bulkInsert = store.BulkInsert())
        {
            foreach (var dto in localDtos)
                bulkInsert.Store(dto);
        }

        new Index().Execute(store);
        new LuceneIndex().Execute(store);
        Indexes.WaitForIndexing(store, timeout: TimeSpan.FromMinutes(5));
        using var session = store.OpenSession();
        var resultsCorax = session.Query<Dto, Index>().OrderBy(x => x.Title, OrderingType.AlphaNumeric).ToList();
        var resultsLucene = session.Query<Dto, LuceneIndex>().OrderBy(x => x.Title, OrderingType.AlphaNumeric).ToList();
        WaitForUserToContinueTheTest(store);
        Assert.Equal(resultsCorax.Count, resultsLucene.Count);
        Assert.Equal(resultsLucene.Select(x => x.Id), resultsCorax.Select(x => x.Id));
    }

    private class LuceneIndex : Index
    {
        public override string IndexName => nameof(LuceneIndex);

        public LuceneIndex() : base()
        {
            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Lucene;
        }
    }

    private class Index : AbstractIndexCreationTask<Dto>
    {
        public Index()
        {
            Map = dtos => from dto in dtos select new { dto.Title };
        }
    }
}
