using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq.Indexing;
using Tests.Infrastructure;
using Xunit;

namespace StressTests.Corax;

public class ScoreSorting(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void SortAllGivesSameOutputAsHeap()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax, includeScoresAndDistances: true));
        using (var bulkInsert = store.BulkInsert())
        {
            for (var i = 0; i < 6_000; i++)
                bulkInsert.Store(CreateItem(i), $"items/{i}");
        }

        new ItemsIndex().Execute(store);
        new BoostedItemsIndex().Execute(store);
        Indexes.WaitForIndexing(store);

        (string Query, int Count, ulong Checksum)[] expected =
        [
            ("from index 'ItemsIndex' where Tags = 't-common' order by score()", 3000, 7355500367956253991UL),
            ("from index 'ItemsIndex' where Tags = 't-common' order by score() desc", 3000, 3861429251504011161UL),
            ("from index 'ItemsIndex' where Tags = 't-common' or startsWith(Tags, 't-rare-1') order by score()", 3360, 4610245765269366717UL),
            ("from index 'ItemsIndex' where Tags = 't-common' or startsWith(Tags, 't-rare-1') order by score() limit 10", 10, 7310777009750505053UL),
            ("from index 'ItemsIndex' where Tags = 't-common' or startsWith(Tags, 't-rare-1') order by score() limit 2000", 2000, 9626466598007701386UL),
            ("from index 'ItemsIndex' where Tags = 't-common' or startsWith(Tags, 't-rare-1') order by score() limit 20 offset 3000", 20, 9750801567049317820UL),
            ("from index 'ItemsIndex' where exact(vector.search(Vector, $vector, 0, 10000)) order by score()", 6000, 7702092843344769729UL),
            ("from index 'ItemsIndex' where exact(vector.search(Vector, $vector, 0, 10000)) order by score() desc", 6000, 8190511138698627195UL),
            ("from index 'BoostedItemsIndex' where exact(vector.search(Vector, $vector, 0, 10000)) order by score()", 6000, 8235958692031983682UL),
            ("from index 'BoostedItemsIndex' where exact(vector.search(Vector, $vector, 0, 10000)) order by score() desc", 6000, 17701206790820302678UL),
            ("from index 'ItemsIndex' where exact(vector.search(Vector, $zero, 0, 10000)) order by score()", 6000, 6910106147959475921UL),
            ("from index 'ItemsIndex' where exact(vector.search(Vector, $zero, 0, 10000)) order by score() desc", 6000, 9449393181387349001UL)
        ];

        var actual = expected.Select(x => Query(store, x.Query)).ToArray();
        Assert.Equal(expected, actual);
    }

    private static (string Query, int Count, ulong Checksum) Query(IDocumentStore store, string query)
    {
        using var session = store.OpenSession();
        var results = session.Advanced.RawQuery<Item>(query)
            .AddParameter("vector", new[] { 1f, 0f })
            .AddParameter("zero", new[] { 0f, 0f })
            .ToList();

        var checksum = 14695981039346656037UL;
        foreach (var item in results)
        {
            foreach (var c in item.Id)
                checksum = (checksum ^ c) * 1099511628211UL;

            var score = (float)session.Advanced.GetMetadataFor(item).GetDouble(Raven.Client.Constants.Documents.Metadata.IndexScore);
            checksum = (checksum ^ (uint)BitConverter.SingleToInt32Bits(score)) * 1099511628211UL;
        }

        return (query, results.Count, checksum);
    }

    private static Item CreateItem(int i)
    {
        var tags = new List<string> { $"t-rare-{i % 100}" };
        if (i % 2 == 0)
            tags.AddRange(Enumerable.Repeat("t-common", 1 + i / 2 % 4));

        return new Item
        {
            Tags = tags.ToArray(),
            Category = i % 3 == 0 ? "c1" : "c2",
            Boost = i % 7 == 0 ? 0 : 1 + i % 5,
            Vector = i % 50 == 0 ? [0f, 0f] : [MathF.Cos(i), MathF.Sin(i)]
        };
    }

    private class ItemsIndex : AbstractIndexCreationTask<Item>
    {
        public ItemsIndex()
        {
            Map = items => from item in items
                select new { item.Tags, item.Category, Vector = CreateVector(item.Vector) };
        }
    }

    private class BoostedItemsIndex : AbstractIndexCreationTask<Item>
    {
        public BoostedItemsIndex()
        {
            Map = items => from item in items
                select new { item.Tags, item.Category, Vector = CreateVector(item.Vector) }.Boost(item.Boost);
        }
    }

    private class Item
    {
        public string Id { get; set; }
        public string[] Tags { get; set; }
        public string Category { get; set; }
        public float Boost { get; set; }
        public float[] Vector { get; set; }
    }
}
