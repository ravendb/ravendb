using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.Documents.Indexes.Debugging;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_27347 : RavenTestBase
{
    public RavenDB_27347(ITestOutputHelper output) : base(output)
    {
    }

    // Constants.Tree.NodeMaxSize == (8192 - 64) / 2 - 1 == 4063 and Constants.Tree.NodeHeaderSize == 11,
    // so Tree.ShouldGoToOverflowPage is true for any map result of 4053 bytes or more. Such a map
    // result is stored as an overflow value in the map-reduce results tree.
    private const int LargePayloadSize = 6000;

    private const int SmallPayloadSize = 1500;

    // random content is incompressible, which makes the recompression attempted when the leaf gets
    // full fail to make room for the incoming entry. The thrown away attempt is what consumes the
    // deletion tombstone for the first time.
    private const int ChurnPayloadSize = 3000;

    private const int NumberOfSmallItems = 6;

    private const int NumberOfRounds = 6;

    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(30);

    private sealed class Item
    {
        public string Group { get; set; }
        public string Payload { get; set; }
    }

    private sealed class Items_ByGroup : AbstractIndexCreationTask<Item, Items_ByGroup.Result>
    {
        public sealed class Result
        {
            public string Group { get; set; }
            public int Count { get; set; }
            public string Payload { get; set; }
        }

        public Items_ByGroup()
        {
            Map = items => from item in items
                           select new { Group = item.Group, Count = 1, Payload = item.Payload };

            Reduce = results => from result in results
                                group result by result.Group
                                into g
                                select new { Group = g.Key, Count = g.Sum(x => x.Count), Payload = g.First().Payload };
        }
    }

    private static string RandomPayload(Random random)
    {
        var chars = new char[ChurnPayloadSize];
        for (int i = 0; i < chars.Length; i++)
            chars[i] = (char)('a' + random.Next(26));
        return new string(chars);
    }

    [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Voron)]
    public async Task IndexingDocumentsWithLargeMapResultsMustKeepWorkingWhenSomeOfThemAreDeleted()
    {
        using (var store = GetDocumentStore())
        {
            var index = new Items_ByGroup();
            await index.ExecuteAsync(store);

            // every document maps to the same reduce key, so all of the map results live in one
            // map-reduce results tree. The large map result is stored as an overflow value.
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Item { Group = "all", Payload = new string('L', LargePayloadSize) }, "large/0");

                await session.SaveChangesAsync();
            }

            await Indexes.WaitForIndexingAsync(store, allowErrors: true, timeout: WaitTimeout);

            // ordinary sized documents - their map results are stored inline and are what actually
            // fills the leaf, which makes the leaf compress and take the overflow node with it
            using (var session = store.OpenAsyncSession())
            {
                for (int i = 0; i < NumberOfSmallItems; i++)
                    await session.StoreAsync(new Item { Group = "all", Payload = new string('s', SmallPayloadSize) }, $"small/{i}");

                await session.SaveChangesAsync();
            }

            await Indexes.WaitForIndexingAsync(store, allowErrors: true);

            // the large document goes away - its map result entry gets a compression tombstone
            using (var session = store.OpenAsyncSession())
            {
                session.Delete("large/0");

                await session.SaveChangesAsync();
            }

            await Indexes.WaitForIndexingAsync(store, allowErrors: true);

            // day to day churn - new documents keep arriving and filling the leaf
            var random = new Random(27347);

            for (int round = 0; round < NumberOfRounds; round++)
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Item { Group = "all", Payload = RandomPayload(random) }, $"churn/{round}");

                    await session.SaveChangesAsync();
                }

                await Indexes.WaitForIndexingAsync(store, allowErrors: true);
            }

            var errors = await store.Maintenance.SendAsync(new GetIndexErrorsOperation(new[] { index.IndexName }));
            var first = errors.SelectMany(x => x.Errors).FirstOrDefault();

            Assert.True(first == null, $"The index failed while indexing ordinary documents: {first?.Error}");

            var expected = NumberOfSmallItems + NumberOfRounds;

            using (var session = store.OpenSession())
            {
                var results = session.Query<Items_ByGroup.Result, Items_ByGroup>().ToList();

                Assert.Equal(1, results.Count);
                Assert.Equal(expected, results[0].Count);
            }

            // in Release builds nothing throws at the fault point - the double consumption of the
            // deletion tombstone silently corrupts the map results tree header (the entry and page
            // accounting the map-reduce visualizer displays)
            var database = await GetDatabase(store.Database);
            var serverSideIndex = database.IndexStore.GetIndex(index.IndexName);

            using (serverSideIndex.GetReduceTree(new[] { "small/0" }, out IEnumerable<ReduceTree> trees))
            {
                var tree = trees.Single();

                Assert.Equal(expected, tree.NumberOfEntries);
            }
        }
    }

    [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Voron)]
    public async Task IndexingManyDocumentsWithLargeMapResultsMustKeepWorkingWhenTheyAreDeletedOneByOne()
    {
        // issue description variant (v8.0 repro); on 6.2 the recompression succeeds, so it only guards the query count
        const int numberOfLargeItems = 60;
        const int numberOfRounds = 50;

        using (var store = GetDocumentStore())
        {
            var index = new Items_ByGroup();
            await index.ExecuteAsync(store);

            // every document maps to the same reduce key, so all of the map results live in one
            // map-reduce results tree. These map results are over Constants.Tree.NodeMaxSize, so the
            // tree stores each of them as an overflow value.
            using (var session = store.OpenAsyncSession())
            {
                for (int i = 0; i < numberOfLargeItems; i++)
                    await session.StoreAsync(new Item { Group = "all", Payload = new string('L', LargePayloadSize) }, $"large/{i}");

                await session.SaveChangesAsync();
            }

            await Indexes.WaitForIndexingAsync(store, allowErrors: true, timeout: WaitTimeout);

            // ordinary sized documents - their map results are stored inline and are what actually
            // fills the leaf, which makes the leaf compress and take the overflow nodes with it
            using (var session = store.OpenAsyncSession())
            {
                for (int i = 0; i < NumberOfSmallItems; i++)
                    await session.StoreAsync(new Item { Group = "all", Payload = new string('s', SmallPayloadSize) }, $"small/{i}");

                await session.SaveChangesAsync();
            }

            await Indexes.WaitForIndexingAsync(store, allowErrors: true);

            // day to day churn: an old document goes away and a new one arrives
            for (int round = 0; round < numberOfRounds; round++)
            {
                using (var session = store.OpenAsyncSession())
                {
                    session.Delete($"large/{round}");

                    await session.StoreAsync(new Item { Group = "all", Payload = new string('s', SmallPayloadSize) }, $"small/{NumberOfSmallItems + round}");

                    await session.SaveChangesAsync();
                }

                await Indexes.WaitForIndexingAsync(store, allowErrors: true);
            }

            var errors = await store.Maintenance.SendAsync(new GetIndexErrorsOperation(new[] { index.IndexName }));
            var first = errors.SelectMany(x => x.Errors).FirstOrDefault();

            Assert.True(first == null, $"The index failed while indexing ordinary documents: {first?.Error}");

            var expected = numberOfLargeItems - numberOfRounds + NumberOfSmallItems + numberOfRounds;

            using (var session = store.OpenSession())
            {
                var results = session.Query<Items_ByGroup.Result, Items_ByGroup>().ToList();

                Assert.Equal(1, results.Count);
                Assert.Equal(expected, results[0].Count);
            }
        }
    }
}
