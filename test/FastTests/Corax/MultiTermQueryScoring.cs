using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Corax;

public class MultiTermQueryScoring(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void ScoresOfEveryTermProviderMatchRecordedValues()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax, includeScoresAndDistances: true));
        using (var bulkInsert = store.BulkInsert())
        {
            for (var i = 0; i < 6_000; i++)
                bulkInsert.Store(CreateItem(i), $"items/{i}");
        }

        new ItemsIndex().Execute(store);
        Indexes.WaitForIndexing(store);

        (string Where, int Count, ulong Checksum)[] expected =
        [
            ("startsWith(Tags, 't-')", 6000, 17899215015677400292UL),
            ("endsWith(Tags, '0')", 600, 10672462111655024232UL),
            ("search(Tags, '*once-00*')", 100, 5089464552136987847UL),
            ("search(Tags, '*once-00?0')", 10, 12694063590607656287UL),
            ("regex(Tags, '^t-(once-00[0-4]|small|set)')", 4022, 11903305927594073393UL),
            ("Ts between 0 and 20000", 6000, 10194025299602325463UL),
            ("Name between 'n-0000' and 'n-3000'", 3030, 11471452644262975143UL),
            ("Code in ('c-set', 'c-small', 'c-3001', 'c-4001', 'c-missing', null)", 3035, 2523237263917149757UL),
            ("startsWith(Tags, 't-once-00') or Category = 'c1'", 2066, 12237141458522969017UL)
        ];

        var actual = expected.Select(x => Query(store, x.Where)).ToArray();

        Assert.Equal(expected, actual);
    }

    private static (string Where, int Count, ulong Checksum) Query(IDocumentStore store, string where)
    {
        using var session = store.OpenSession();
        var results = session.Advanced
            .RawQuery<Item>($"from index '{new ItemsIndex().IndexName}' where {where} order by score()")
            .ToList();

        var checksum = 14695981039346656037UL;
        foreach (var item in results.OrderBy(x => x.Id, StringComparer.Ordinal))
        {
            var score = (float)(double)session.Advanced.GetMetadataFor(item)[Raven.Client.Constants.Documents.Metadata.IndexScore];
            checksum = (checksum ^ (uint)BitConverter.SingleToInt32Bits(score)) * 1099511628211UL;
        }

        return (where, results.Count, checksum);
    }

    private static Item CreateItem(int i)
    {
        var tags = new List<string> { $"t-once-{i:D4}" };
        if (i < 4_000)
            tags.AddRange(Enumerable.Repeat("t-set", 1 + i % 4));
        if (i % 100 == 0)
            tags.Add("t-small-a");
        if (i % 997 == 0)
            tags.Add("t-small-b");
        if (i == 10)
            tags.Add("t-once-x10");
        if (i == 20)
            tags.AddRange(Enumerable.Repeat("t-once-0020", 2));
        if (i == 40)
            tags.AddRange(Enumerable.Repeat("t-once-0040", 19));

        return new Item
        {
            Tags = tags.ToArray(),
            Ts = i < 3_000 ? 7 : i % 100 == 50 ? 8 : 10_000 + i,
            Name = i % 100 == 0 ? "n-0000" : $"n-{i:D4}",
            Code = i % 1_000 == 999 ? null : i < 3_000 ? "c-set" : i % 100 == 50 ? "c-small" : $"c-{i:D4}",
            Category = i % 3 == 0 ? "c1" : "c2"
        };
    }

    private class ItemsIndex : AbstractIndexCreationTask<Item>
    {
        public ItemsIndex()
        {
            Map = items => from item in items
                select new { item.Tags, item.Ts, item.Name, item.Code, item.Category };
        }
    }

    private class Item
    {
        public string Id { get; set; }
        public string[] Tags { get; set; }
        public long Ts { get; set; }
        public string Name { get; set; }
        public string Code { get; set; }
        public string Category { get; set; }
    }
}
