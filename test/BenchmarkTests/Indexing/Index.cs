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
using Xunit.Abstractions;

namespace BenchmarkTests.Indexing
{
    public class Map : BenchmarkTestBase
    {
        public Map(ITestOutputHelper output) : base(output)
        {
        }

        private const int NumberOfCompanies = 5_000_000;

        private const int CompanyNameModulo = 10_000;

        private const string IndexDatabaseName = "Index_Companies";

        private const string ReIndexDatabaseName = "ReIndex_Companies";

        [Fact]
        public async Task Simple_Map()
        {
            using (var store = GetSimpleDocumentStore(IndexDatabaseName, deleteDatabaseOnDispose: false))
            {
                var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
                Assert.Equal(NumberOfCompanies + 1, stats.CountOfDocuments); // + hilo

                new Simple_Map_Index().Execute(store);

                await WaitForIndexAsync(store, store.Database, new Simple_Map_Index().IndexName, TimeSpan.FromMinutes(10));

                RavenTestHelper.AssertNoIndexErrors(store);

                var indexStats = await store.Maintenance.SendAsync(new GetIndexStatisticsOperation(new Simple_Map_Index().IndexName));
                Assert.Equal(NumberOfCompanies, indexStats.EntriesCount);
            }
        }

        [Fact]
        public async Task Simple_Map_FullText_Search()
        {
            using (var store = GetSimpleDocumentStore(IndexDatabaseName, deleteDatabaseOnDispose: false))
            {
                var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
                Assert.Equal(NumberOfCompanies + 1, stats.CountOfDocuments); // + hilo

                new Simple_Map_FullText_Search_Index().Execute(store);

                await WaitForIndexAsync(store, store.Database, new Simple_Map_FullText_Search_Index().IndexName, TimeSpan.FromMinutes(60));

                RavenTestHelper.AssertNoIndexErrors(store);

                var indexStats = await store.Maintenance.SendAsync(new GetIndexStatisticsOperation(new Simple_Map_FullText_Search_Index().IndexName));
                Assert.Equal(NumberOfCompanies, indexStats.EntriesCount);
            }
        }

        [Fact]
        public async Task Simple_Map_ReIndex()
        {
            using (var store = GetSimpleDocumentStore(ReIndexDatabaseName, deleteDatabaseOnDispose: false))
            {
                var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
                Assert.Equal(NumberOfCompanies + 1, stats.CountOfDocuments); // + hilo

                var indexingStatus = await store.Maintenance.SendAsync(new GetIndexingStatusOperation());
                Assert.Equal(IndexRunningStatus.Paused, indexingStatus.Status);

                await store.Maintenance.SendAsync(new StartIndexOperation(new Simple_Map_Index().IndexName));

                await WaitForIndexAsync(store, store.Database, new Simple_Map_Index().IndexName, TimeSpan.FromMinutes(10));

                RavenTestHelper.AssertNoIndexErrors(store);

                var indexStats = await store.Maintenance.SendAsync(new GetIndexStatisticsOperation(new Simple_Map_Index().IndexName));
                Assert.Equal(NumberOfCompanies, indexStats.EntriesCount);
            }
        }

        [Fact]
        public async Task Simple_MapReduce()
        {
            using (var store = GetSimpleDocumentStore(IndexDatabaseName, deleteDatabaseOnDispose: true))
            {
                var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
                Assert.Equal(NumberOfCompanies + 1, stats.CountOfDocuments); // + hilo

                new Simple_MapReduce_Index().Execute(store);

                await WaitForIndexAsync(store, store.Database, new Simple_MapReduce_Index().IndexName, TimeSpan.FromMinutes(10));

                RavenTestHelper.AssertNoIndexErrors(store);

                var indexStats = await store.Maintenance.SendAsync(new GetIndexStatisticsOperation(new Simple_MapReduce_Index().IndexName));
                Assert.Equal(CompanyNameModulo, indexStats.EntriesCount);
            }
        }

        [Fact]
        public async Task Simple_MapReduce_ReIndex()
        {
            using (var store = GetSimpleDocumentStore(ReIndexDatabaseName, deleteDatabaseOnDispose: true))
            {
                var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
                Assert.Equal(NumberOfCompanies + 1, stats.CountOfDocuments); // + hilo

                var indexingStatus = await store.Maintenance.SendAsync(new GetIndexingStatusOperation());
                Assert.Equal(IndexRunningStatus.Paused, indexingStatus.Status);

                await store.Maintenance.SendAsync(new StartIndexOperation(new Simple_MapReduce_Index().IndexName));

                await WaitForIndexAsync(store, store.Database, new Simple_MapReduce_Index().IndexName, TimeSpan.FromMinutes(10));

                RavenTestHelper.AssertNoIndexErrors(store);

                var indexStats = await store.Maintenance.SendAsync(new GetIndexStatisticsOperation(new Simple_MapReduce_Index().IndexName));
                Assert.Equal(CompanyNameModulo, indexStats.EntriesCount);
            }
        }

        private class Simple_MapReduce_Index : AbstractIndexCreationTask<Company, Simple_MapReduce_Index.Result>
        {
            public class Result
            {
                public int Count { get; set; }

                public string Name { get; set; }
            }

            public Simple_MapReduce_Index()
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

        private class Simple_Map_Index : AbstractIndexCreationTask<Company>
        {
            public Simple_Map_Index()
            {
                Map = companies => from c in companies
                                   select new
                                   {
                                       Name = c.Name,
                                       Email = c.Email
                                   };
            }
        }

        private class Simple_Map_FullText_Search_Index : AbstractIndexCreationTask<Company>
        {
            public Simple_Map_FullText_Search_Index()
            {
                Map = companies => from c in companies
                    select new
                    {
                        Query = new[]
                        {
                            c.Name,
                            c.Email
                        }
                    };

                Index("Query", FieldIndexing.Search);
            }
        }

        public override async Task InitAsync(DocumentStore store)
        {
            await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(CreateDatabaseRecord(IndexDatabaseName)));
            await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(CreateDatabaseRecord(ReIndexDatabaseName)));

            await new Simple_Map_Index().ExecuteAsync(store, database: ReIndexDatabaseName);
            await new Simple_MapReduce_Index().ExecuteAsync(store, database: ReIndexDatabaseName);

            using (var bulkInsert1 = store.BulkInsert(IndexDatabaseName))
            using (var bulkInsert2 = store.BulkInsert(ReIndexDatabaseName))
            {
                for (int i = 0; i < NumberOfCompanies; i++)
                {
                    var company1 = EntityFactory.CreateCompanySmall(i);
                    company1.Name = $"Hibernating Rhinos {i % CompanyNameModulo}";

                    var company2 = EntityFactory.CreateCompanySmall(i);
                    company2.Name = $"Hibernating Rhinos {i % CompanyNameModulo}";

                    await bulkInsert1.StoreAsync(company1);

                    await bulkInsert2.StoreAsync(company2);
                }
            }

            WaitForIndexing(store, ReIndexDatabaseName, timeout: TimeSpan.FromMinutes(10));

            await store.Maintenance.ForDatabase(ReIndexDatabaseName).SendAsync(new StopIndexingOperation());

            var operation = await store
                .Operations
                .ForDatabase(ReIndexDatabaseName)
                .SendAsync(new PatchByQueryOperation("from Companies update { this.Name = this.Name + '_patched'; }"));

            await operation.WaitForCompletionAsync();
        }
    }
}
