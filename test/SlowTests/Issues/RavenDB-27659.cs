using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Session;
using Raven.Server.Config;
using Raven.Server.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_27659 : RavenTestBase
    {
        private const int MapBatchSize = 128; // the minimum allowed map batch size
        private const int ItemsCount = MapBatchSize * 3;
        private const int ValuesCount = 10;
        private static readonly int ItemsReferencingFirstValue = Enumerable.Range(0, ItemsCount).Count(i => i % ValuesCount == 0);

        public RavenDB_27659(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task CompareExchangeValuesThatExistedOnIndexCreation_ShouldNotBeHandledAsReferences(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                await StoreValuesAndItems(store);

                var batches = await CreateIndexAndRunBatches<Items_ByValue>(store);

                Assert.Equal(ItemsCount, batches.Sum(x => x.MapAttempts));
                Assert.Equal(0, batches.Sum(x => x.MapReferenceAttempts));

                using (var session = store.OpenAsyncSession())
                {
                    var count = await session.Query<Items_ByValue.Result, Items_ByValue>().Where(x => x.Name == "value-0").CountAsync();
                    Assert.Equal(ItemsReferencingFirstValue, count);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task DocumentsAndCompareExchangeValuesThatExistedOnIndexCreation_ShouldNotBeHandledAsReferences(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                await StoreValuesAndItems(store);

                var batches = await CreateIndexAndRunBatches<Items_ByValueAndEmployee>(store);

                Assert.Equal(ItemsCount, batches.Sum(x => x.MapAttempts));
                Assert.Equal(0, batches.Sum(x => x.MapReferenceAttempts));

                using (var session = store.OpenAsyncSession())
                {
                    var count = await session.Query<Items_ByValueAndEmployee.Result, Items_ByValueAndEmployee>().Where(x => x.LastName == "Before").CountAsync();
                    Assert.Equal(ItemsCount, count);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task CompareExchangeValueChangedAfterIndexCreation_ShouldBeHandledOnceAfterTheMap(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                await StoreValuesAndItems(store);

                var batches = await CreateIndexAndRunBatches<Items_ByValue>(store, beforeIndexing: async () =>
                {
                    using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                    {
                        var value = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<Value>("values/0");
                        value.Value.Name = "changed";
                        await session.SaveChangesAsync();
                    }
                });

                Assert.Equal(ItemsCount, batches.Sum(x => x.MapAttempts));
                Assert.Equal(ItemsReferencingFirstValue, batches.Sum(x => x.MapReferenceAttempts));

                var lastMapBatch = batches.FindLastIndex(x => x.MapAttempts > 0);
                var firstReferencesBatch = batches.FindIndex(x => x.MapReferenceAttempts > 0);
                Assert.True(lastMapBatch < firstReferencesBatch, $"references were handled in batch {firstReferencesBatch} while the map finished only in batch {lastMapBatch}");

                using (var session = store.OpenAsyncSession())
                {
                    var count = await session.Query<Items_ByValue.Result, Items_ByValue>().Where(x => x.Name == "changed").CountAsync();
                    Assert.Equal(ItemsReferencingFirstValue, count);
                }
            }
        }

        private static async Task StoreValuesAndItems(IDocumentStore store)
        {
            for (var i = 0; i < ValuesCount; i++)
                await store.Operations.SendAsync(new PutCompareExchangeValueOperation<Value>($"values/{i}", new Value { Name = $"value-{i}" }, 0));

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Employee { LastName = "Before" }, "employees/1");

                for (var i = 0; i < ItemsCount; i++)
                    await session.StoreAsync(new Item { ValueKey = $"values/{i % ValuesCount}", EmployeeId = "employees/1" }, $"items/{i}");

                await session.SaveChangesAsync();
            }
        }

        private async Task<List<(long MapAttempts, long MapReferenceAttempts)>> CreateIndexAndRunBatches<TIndex>(IDocumentStore store, Func<Task> beforeIndexing = null)
            where TIndex : AbstractIndexCreationTask, new()
        {
            // the index is created while indexing is stopped, so the test runs its batches one by one
            await store.Maintenance.SendAsync(new StopIndexingOperation());

            var index = new TIndex();
            index.Configuration[RavenConfiguration.GetKey(x => x.Indexing.MapBatchSize)] = MapBatchSize.ToString();
            await index.ExecuteAsync(store);

            if (beforeIndexing != null)
                await beforeIndexing();

            var database = await GetDatabase(store.Database);
            var indexInstance = database.IndexStore.GetIndex(index.IndexName);

            var batches = new List<(long MapAttempts, long MapReferenceAttempts)>();
            bool moreWork;
            do
            {
                var stats = new IndexingRunStats();
                moreWork = indexInstance.DoIndexingWork(new IndexingStatsScope(stats), CancellationToken.None);
                batches.Add((stats.MapAttempts, stats.MapReferenceAttempts));

                Assert.True(batches.Count <= 50, $"the index didn't finish indexing after {batches.Count} batches");
            } while (moreWork);

            var batchesDescription = string.Join(", ", batches.Select((x, i) => $"batch {i}: map {x.MapAttempts}, references {x.MapReferenceAttempts}"));
            Assert.True(batches.Count(x => x.MapAttempts > 0) == ItemsCount / MapBatchSize, $"the map should have needed {ItemsCount / MapBatchSize} batches. {batchesDescription}");

            return batches;
        }

        private class Value
        {
            public string Name { get; set; }
        }

        private class Item
        {
            public string ValueKey { get; set; }

            public string EmployeeId { get; set; }
        }

        private class Employee
        {
            public string LastName { get; set; }
        }

        private class Items_ByValue : AbstractIndexCreationTask<Item>
        {
            public class Result
            {
                public string Name { get; set; }
            }

            public Items_ByValue()
            {
                Map = items => from item in items
                               select new Result
                               {
                                   Name = LoadCompareExchangeValue<Value>(item.ValueKey).Name
                               };
            }
        }

        private class Items_ByValueAndEmployee : AbstractIndexCreationTask<Item>
        {
            public class Result
            {
                public string Name { get; set; }

                public string LastName { get; set; }
            }

            public Items_ByValueAndEmployee()
            {
                Map = items => from item in items
                               select new Result
                               {
                                   Name = LoadCompareExchangeValue<Value>(item.ValueKey).Name,
                                   LastName = LoadDocument<Employee>(item.EmployeeId).LastName
                               };
            }
        }
    }
}
