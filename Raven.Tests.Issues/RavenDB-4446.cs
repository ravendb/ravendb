// -----------------------------------------------------------------------
//  <copyright file="RavenDB-4446.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Client.Indexes;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_4446 : RavenTest
    {
        [Fact]
        public void can_disable_precomputed_batch()
        {
            using (var store = NewDocumentStore(configureStore: documentStore =>
            {
                documentStore.Configuration.MaxPrecomputedBatchSizeForNewIndex = 0;
            }))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order{CompanyId = "companies/1"});
                    session.SaveChanges();
                }

                var index = new Orders_Index();
                index.Execute(store);

                WaitForIndexing(store);
            }
        }

        [Fact]
        public void can_index_collections_larger_than_32768()
        {
            using (var store = NewRemoteDocumentStore())
            {
                using (var bulk = store.BulkInsert())
                {
                    for(var i = 0; i < 40000; i++)
                        bulk.Store(new Order { CompanyId = i.ToString() });
                }

                //wait for indexing of Raven/DocumentsByEntityName
                WaitForIndexing(store);

                var index = new Orders_Index();
                index.Execute(store);

                WaitForIndexing(store);

                var stats = store.DatabaseCommands.GetStatistics();
                var ordersIndex = stats.Indexes.First(x => x.ForEntityName.Contains("Orders"));
                Assert.Equal(40000, ordersIndex.DocsCount);
            }
        }

        [Fact]
        public void can_index_collections_smaller_than_32768()
        {
            using (var store = NewDocumentStore())
            {
                using (var bulk = store.BulkInsert())
                {
                    for (var i = 0; i < 30000; i++)
                        bulk.Store(new Order { CompanyId = i.ToString() });
                }

                //wait for indexing of Raven/DocumentsByEntityName
                WaitForIndexing(store);

                var index = new Orders_Index();
                index.Execute(store);

                WaitForIndexing(store);

                var stats = store.DatabaseCommands.GetStatistics();
                var ordersIndex = stats.Indexes.First(x => x.ForEntityName.Contains("Orders"));
                Assert.Equal(30000, ordersIndex.DocsCount);
            }
        }

        [Fact]
        public void can_index_collections_larger_than_predefined()
        {
            using (var store = NewDocumentStore(configureStore: documentStore =>
            {
                documentStore.Configuration.MaxPrecomputedBatchSizeForNewIndex = 10000;
            }))
            {
                using (var bulk = store.BulkInsert())
                {
                    for (var i = 0; i < 10500; i++)
                        bulk.Store(new Order { CompanyId = i.ToString() });
                }

                //wait for indexing of Raven/DocumentsByEntityName
                WaitForIndexing(store);

                var index = new Orders_Index();
                index.Execute(store);

                WaitForIndexing(store);

                var stats = store.DatabaseCommands.GetStatistics();
                var ordersIndex = stats.Indexes.First(x => x.ForEntityName.Contains("Orders"));
                Assert.Equal(10500, ordersIndex.DocsCount);
            }
        }

        [Fact]
        public void can_index_collections_smaller_than_predefined()
        {
            using (var store = NewDocumentStore(configureStore: documentStore =>
            {
                documentStore.Configuration.MaxPrecomputedBatchSizeForNewIndex = 9000;
            }))
            {
                using (var bulk = store.BulkInsert())
                {
                    for (var i = 0; i < 8999; i++)
                        bulk.Store(new Order { CompanyId = i.ToString() });
                }

                //wait for indexing of Raven/DocumentsByEntityName
                WaitForIndexing(store);

                var index = new Orders_Index();
                index.Execute(store);

                WaitForIndexing(store);

                var stats = store.DatabaseCommands.GetStatistics();
                var ordersIndex = stats.Indexes.First(x => x.ForEntityName.Contains("Orders"));
                Assert.Equal(8999, ordersIndex.DocsCount);
            }
        }

        public class Order
        {
            public string Id { get; set; }
            public string CompanyId { get; set; }
        }

        public class Orders_Index : AbstractIndexCreationTask<Order>
        {
            public Orders_Index()
            {
                Map = orders => from order in orders
                               select new
                               {
                                   order.CompanyId
                               };
            }
        }
    }
}
