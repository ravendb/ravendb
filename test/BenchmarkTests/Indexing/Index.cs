using System;
using System.Linq;
using System.Threading.Tasks;
using BenchmarkTests.Utils;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace BenchmarkTests.Indexing
{
    public class Map : BenchmarkTestBase
    {
        [Fact]
        public async Task Simple_Map_1M()
        {
            using (var store = GetSimpleDocumentStore("1M_Companies", deleteDatabaseOnDispose: false))
            {
                var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
                Assert.Equal(1_000_001, stats.CountOfDocuments); // + hilo

                new Simple_Map().Execute(store);

                await WaitForIndexAsync(store, store.Database, new Simple_Map().IndexName, TimeSpan.FromMinutes(10));

                RavenTestHelper.AssertNoIndexErrors(store);

                var indexStats = await store.Maintenance.SendAsync(new GetIndexStatisticsOperation(new Simple_Map().IndexName));
                Assert.Equal(1_000_000, indexStats.EntriesCount);
            }
        }

        [Fact]
        public async Task Simple_Map_1M_ReIndex()
        {
            using (var store = GetSimpleDocumentStore("1M_Companies_ReIndex", deleteDatabaseOnDispose: false))
            {
                var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
                Assert.Equal(1_000_001, stats.CountOfDocuments); // + hilo

                var indexingStatus = await store.Maintenance.SendAsync(new GetIndexingStatusOperation());
                Assert.Equal(IndexRunningStatus.Paused, indexingStatus.Status);

                await store.Maintenance.SendAsync(new StartIndexOperation(new Simple_Map().IndexName));

                await WaitForIndexAsync(store, store.Database, new Simple_Map().IndexName, TimeSpan.FromMinutes(10));

                RavenTestHelper.AssertNoIndexErrors(store);

                var indexStats = await store.Maintenance.SendAsync(new GetIndexStatisticsOperation(new Simple_Map().IndexName));
                Assert.Equal(1_000_000, indexStats.EntriesCount);
            }
        }

        [Fact]
        public async Task Simple_MapReduce_1M()
        {
            using (var store = GetSimpleDocumentStore("1M_Companies", deleteDatabaseOnDispose: true))
            {
                var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
                Assert.Equal(1_000_001, stats.CountOfDocuments); // + hilo

                new Simple_MapReduce().Execute(store);

                await WaitForIndexAsync(store, store.Database, new Simple_MapReduce().IndexName, TimeSpan.FromMinutes(10));

                RavenTestHelper.AssertNoIndexErrors(store);

                var indexStats = await store.Maintenance.SendAsync(new GetIndexStatisticsOperation(new Simple_MapReduce().IndexName));
                Assert.Equal(10_000, indexStats.EntriesCount);
            }
        }

        [Fact]
        public async Task Simple_MapReduce_1M_ReIndex()
        {
            using (var store = GetSimpleDocumentStore("1M_Companies_ReIndex", deleteDatabaseOnDispose: true))
            {
                var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
                Assert.Equal(1_000_001, stats.CountOfDocuments); // + hilo

                var indexingStatus = await store.Maintenance.SendAsync(new GetIndexingStatusOperation());
                Assert.Equal(IndexRunningStatus.Paused, indexingStatus.Status);

                await store.Maintenance.SendAsync(new StartIndexOperation(new Simple_MapReduce().IndexName));

                await WaitForIndexAsync(store, store.Database, new Simple_MapReduce().IndexName, TimeSpan.FromMinutes(10));

                RavenTestHelper.AssertNoIndexErrors(store);

                var indexStats = await store.Maintenance.SendAsync(new GetIndexStatisticsOperation(new Simple_MapReduce().IndexName));
                Assert.Equal(10_000, indexStats.EntriesCount);
            }
        }

        private class Simple_MapReduce : AbstractIndexCreationTask<Company, Simple_MapReduce.Result>
        {
            public class Result
            {
                public int Count { get; set; }

                public string Name { get; set; }
            }

            public Simple_MapReduce()
            {
                Map = companies => from c in companies
                                   select new
                                   {
                                       Name = c.Name,
                                       Count = 1
                                   };

                Reduce = results => from r in results
                                    group r by r.Name into g
                                    select new
                                    {
                                        Name = g.Key,
                                        Count = g.Sum(x => x.Count)
                                    };
            }
        }

        private class Simple_Map : AbstractIndexCreationTask<Company>
        {
            public Simple_Map()
            {
                Map = companies => from c in companies
                                   select new
                                   {
                                       Name = c.Name,
                                       Email = c.Email
                                   };
            }
        }

        public override async Task InitAsync(DocumentStore store)
        {
            await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord("1M_Companies")));
            await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord("1M_Companies_ReIndex")));

            await new Simple_Map().ExecuteAsync(store, database: "1M_Companies_ReIndex");
            await new Simple_MapReduce().ExecuteAsync(store, database: "1M_Companies_ReIndex");

            using (var bulkInsert1 = store.BulkInsert("1M_Companies"))
            using (var bulkInsert2 = store.BulkInsert("1M_Companies_ReIndex"))
            {
                for (int i = 0; i < 1_000_000; i++)
                {
                    var company1 = EntityFactory.CreateCompanySmall(i);
                    company1.Name = $"Hibernating Rhinos {i % 10_000}";

                    var company2 = EntityFactory.CreateCompanySmall(i);
                    company2.Name = $"Hibernating Rhinos {i % 10_000}";

                    await bulkInsert1.StoreAsync(company1);

                    await bulkInsert2.StoreAsync(company2);
                }
            }

            WaitForIndexing(store, "1M_Companies_ReIndex", timeout: TimeSpan.FromMinutes(10));

            await store.Maintenance.ForDatabase("1M_Companies_ReIndex").SendAsync(new StopIndexingOperation());

            var operation = await store
                .Operations
                .ForDatabase("1M_Companies_ReIndex")
                .SendAsync(new PatchByQueryOperation("from Companies update { this.Name = this.Name + '_patched'; }"));

            await operation.WaitForCompletionAsync();
        }
    }
}
