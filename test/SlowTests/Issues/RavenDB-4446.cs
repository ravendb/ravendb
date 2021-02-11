// -----------------------------------------------------------------------
//  <copyright file="RavenDB-4446.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_4446 : RavenTestBase
    {
        public RavenDB_4446(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void can_index_collections_larger_than_32768()
        {
            using (var store = GetDocumentStore())
            {
                using (var bulk = store.BulkInsert())
                {
                    for (var i = 0; i < 40000; i++)
                        bulk.Store(new Order { CompanyId = i.ToString() });
                }

                //wait for indexing of Raven/DocumentsByEntityName
                WaitForIndexing(store);

                var index = new Orders_Index();
                index.Execute(store);

                WaitForIndexing(store);

                Assert.Equal(40000, WaitForEntriesCount(store, index.IndexName, 40000));
            }
        }

        [Fact]
        public void can_index_collections_smaller_than_32768()
        {
            using (var store = GetDocumentStore())
            {
                using (var bulk = store.BulkInsert())
                {
                    for (var i = 0; i < 30000; i++)
                        bulk.Store(new Order { CompanyId = i.ToString() });
                }

                var index = new Orders_Index();
                index.Execute(store);

                WaitForIndexing(store);

                Assert.Equal(30000, WaitForEntriesCount(store, index.IndexName, 30000));
            }
        }

        private class Order
        {
            public string Id { get; set; }
            public string CompanyId { get; set; }
        }

        private class Orders_Index : AbstractIndexCreationTask<Order>
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
