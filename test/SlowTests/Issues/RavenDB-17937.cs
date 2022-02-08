using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using FastTests.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17937 : RavenTestBase
    {
        public RavenDB_17937(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [InlineData("Index")]
        [InlineData("ZIndex")]
        public async Task Can_Add_Database_Folder_With_Side_By_Side_Indexes(string indexName)
        {
            var path = NewDataPath();

            using (var store = GetDocumentStore(new Options
                   {
                       RunInMemory = false,
                       Path = path
            }))
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Query.Order());
                    await session.SaveChangesAsync();
                }

                var index1 = new Index1(indexName);
                await index1.ExecuteAsync(store);
                await new Index2(indexName).ExecuteAsync(store);

                WaitForIndexing(store, allowErrors: true, timeout: TimeSpan.FromSeconds(5));

                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                Assert.Equal(1, record.Indexes.Count);
                Assert.Equal(index1.IndexName, record.Indexes.First().Key);

                var indexStats = await store.Maintenance.SendAsync(new GetIndexesStatisticsOperation());
                Assert.Equal(2, indexStats.Length);

                await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(store.Database, hardDelete: false));

                await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(store.Database)
                {
                    Settings =
                    {
                        {RavenConfiguration.GetKey(x => x.Core.RunInMemory), "false" },
                        {RavenConfiguration.GetKey(x => x.Core.DataDirectory), path }
                    }
                }));

                record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                Assert.Equal(index1.IndexName, record.Indexes.First().Key);
                Assert.Contains("Count = 1 / this0.x", record.Indexes.First().Value.Maps.First());

                indexStats = await store.Maintenance.SendAsync(new GetIndexesStatisticsOperation());
                Assert.Equal(2, indexStats.Length);
            }
        }

        [Fact]
        public async Task Throw_If_Creating_A_Database_With_Side_By_Side_Indexes()
        {
            using (var store = GetDocumentStore())
            {
                var indexName = $"{Raven.Client.Constants.Documents.Indexing.SideBySideIndexNamePrefix}test";
                var error = await Assert.ThrowsAsync<RavenException>(async () =>
                {
                    await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(GetDatabaseName())
                    {
                        Indexes = new Dictionary<string, IndexDefinition>
                        {
                            {
                                indexName,
                                new IndexDefinition
                                {
                                    Name = indexName,
                                    Maps = new HashSet<string>
                                    {
                                        "docs.Orders.Select(order => new {\r\n    order = order,\r\n    x = 0\r\n}).Select(this0 => new {\r\n    Count = 1 / this0.x\r\n})"
                                    }
                                }
                            }
                        }
                    }));
                });

                Assert.Contains("Index name cannot start with ReplacementOf/", error.Message);
            }
        }

        private class Index1 : AbstractIndexCreationTask<Query.Order>
        {
            public override string IndexName { get; }

            public Index1(string indexName)
            {
                IndexName = indexName;

                Map = orders =>
                    from order in orders
                    select new
                    {
                        Count = 1
                    };
            }
        }

        private class Index2 : AbstractIndexCreationTask<Query.Order>
        {
            public override string IndexName { get; }

            public Index2(string indexName)
            {
                IndexName = indexName;

                Map = orders =>
                    from order in orders
                    let x = 0
                    select new
                    {
                        Count = 1 / x
                    };
            }
        }
    }
}
