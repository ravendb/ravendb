using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.Config;
using Raven.Server.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15746 : RavenTestBase
    {
        public RavenDB_15746(ITestOutputHelper output) : base(output)
        {
        }
        
        [Fact]
        public async Task ChangingThrottlingConfigurationDoesNotRequireIndexReset()
        {
            using (var store = GetDocumentStore())
            {
                var indexDef = new Orders_ByOrderedAtAndShippedAt();
                await indexDef.ExecuteAsync(store);

                var database = await GetDatabase(store.Database);

                var indexDefWithThrottling = new Orders_ByOrderedAtAndShippedAt
                {
                    Configuration = {[RavenConfiguration.GetKey(x => x.Indexing.ThrottlingTimeInterval)] = "1000"}
                };

                var index = database.IndexStore.GetIndex(indexDef.IndexName);

                var options = database.IndexStore.GetIndexCreationOptions(indexDefWithThrottling.CreateIndexDefinition(), index, out var _);
                Assert.Equal(IndexCreationOptions.UpdateWithoutUpdatingCompiledIndex, options);
            }
        }

        [Fact]
        public async Task ShouldNotThrottleBetweenBatchesWhereThereAreStillDocumentsToProcess()
        {
            using (var store = GetDocumentStore())
            {
                var now = DateTime.Now;

                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 512; i++)
                    {
                        session.Store(new Order { OrderedAt = now.AddDays(i % 20), ShippedAt = null });
                    }
                    
                    session.SaveChanges();
                }

                var index = new Orders_ByOrderedAtAndShippedAt
                {
                    Configuration =
                    {
                        [RavenConfiguration.GetKey(x => x.Indexing.ThrottlingTimeInterval)] = TimeSpan.FromHours(1).TotalMilliseconds.ToString(),
                        [RavenConfiguration.GetKey(x => x.Indexing.MapBatchSize)] = "128", // make sure we'll need multiple batches to index all docs
                    }
                };

                await index.ExecuteAsync(store);

                // should index all documents regardless throttling set to 1 hour
                WaitForIndexing(store, timeout: TimeSpan.FromSeconds(30));
            }
        }

        [Fact]
        public async Task ShouldIndexAllDocumentsWithThrottlingSet()
        {
            using (var store = GetDocumentStore())
            {
                var now = DateTime.Now;

                var index = new Orders_ByOrderedAtAndShippedAt
                {
                    Configuration =
                    {
                        [RavenConfiguration.GetKey(x => x.Indexing.ThrottlingTimeInterval)] = TimeSpan.FromSeconds(5).TotalMilliseconds.ToString(),
                    }
                };

                await index.ExecuteAsync(store);

                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        session.Store(new Order { OrderedAt = now.AddDays(i % 20), ShippedAt = null });
                    }

                    session.SaveChanges();
                }

                var indexingDuration = Stopwatch.StartNew();
                
                WaitForIndexing(store);

                indexingDuration.Stop();

                Assert.True(indexingDuration.Elapsed >= TimeSpan.FromSeconds(4)); 

                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        session.Store(new Order { OrderedAt = now.AddDays(i % 20), ShippedAt = null });
                    }

                    session.SaveChanges();
                }

                indexingDuration.Restart();

                WaitForIndexing(store);

                indexingDuration.Stop();

                Assert.True(indexingDuration.Elapsed >= TimeSpan.FromSeconds(4));
            }
        }

        [Fact]
        public async Task CanGetThrottlingValueFromIndexDefinition()
        {
            using (var store = GetDocumentStore())
            {
                var now = DateTime.Now;

                var index = new Orders_ByOrderedAtAndShippedAt
                {
                    Configuration =
                    {
                        [RavenConfiguration.GetKey(x => x.Indexing.ThrottlingTimeInterval)] = TimeSpan.FromSeconds(5).TotalMilliseconds.ToString(),
                    }
                };

                await index.ExecuteAsync(store);

                var def = store.Maintenance.Send(new GetIndexOperation(index.IndexName));

                Assert.Equal("5000", def.Configuration[RavenConfiguration.GetKey(x => x.Indexing.ThrottlingTimeInterval)]);
            }
        }


        [Fact]
        public async Task IndexMustBeStaleDuringThrottlingTime()
        {
            using (var store = GetDocumentStore())
            {
                var now = DateTime.Now;

                var index = new Orders_ByOrderedAtAndShippedAt
                {
                    Configuration =
                    {
                        [RavenConfiguration.GetKey(x => x.Indexing.ThrottlingTimeInterval)] = TimeSpan.FromHours(1).TotalMilliseconds.ToString(),
                    }
                };

                await index.ExecuteAsync(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Order { OrderedAt = now, ShippedAt = null });
                    session.SaveChanges();
                }

                var stats = store.Maintenance.Send(new GetIndexStatisticsOperation(index.IndexName));

                Assert.True(stats.IsStale);
            }
        }

        private class Orders_ByOrderedAtAndShippedAt : AbstractIndexCreationTask<Order>
        {
            public Orders_ByOrderedAtAndShippedAt()
            {
                Map = orders => from o in orders
                    select new
                    {
                        o.OrderedAt,
                        o.ShippedAt
                    };

            }
        }
    }
}
