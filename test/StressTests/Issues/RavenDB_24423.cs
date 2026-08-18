using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Sparrow;
using Tests.Infrastructure;
using Xunit;

namespace StressTests.Issues;

public class RavenDB_24423(ITestOutputHelper output) : SlowTests.Issues.RavenDB_24423(output)
{
    [RavenMultiplatformTheory(RavenTestCategory.Querying, RavenArchitecture.AllX64)]
    [InlineData(1, 0x007F)]
    [InlineData(0x0080, 0x07FF)]
    [InlineData(0x0800, 0xFFFF)]
    [InlineData(0x010000, 0x10FFFF)]
    public void CoraxAndLuceneAlphanumericalSortResultMustBeIdentical(int from, int toInclusive)
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        var localDtos = new List<Dto>();

        
        for (int i = from; i <= toInclusive; ++i)
        {
            if (i is 1 or 3 || i is >= 0xFFF0 and  <= 0xFFFF) // ignore: https://en.wikipedia.org/wiki/Specials_(Unicode_block)
                continue; // special characters reserved by Corax, we're handling sorting it quite differently so hard to compare the order

           
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
        var luceneResults = session.Query<Dto, LuceneIndex>().OrderBy(x => x.Title, OrderingType.AlphaNumeric).ToList();
        Assert.Equal(luceneResults.Select(x => x.Id), resultsCorax.Select(x => x.Id));
    }
}
