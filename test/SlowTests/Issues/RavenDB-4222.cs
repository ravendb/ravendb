// -----------------------------------------------------------------------
//  <copyright file="RavenDB-4222.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Raven.Client.Indexing;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_4222 : RavenTestBase
    {
        [Fact]
        public void DontUpdateDisabledIndex()
        {
            using (var store = GetDocumentStore())
            {
                var indexName = "test";
                store.DatabaseCommands.PutIndex(indexName, new IndexDefinition
                {
                    Name = indexName,
                    Maps = { @"from doc in docs.Orders
select new{
doc.Name
}" }
                });

                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        Name = indexName
                    }, "orders/1");
                    session.SaveChanges();
                }

                WaitForIndexing(store);
                using (var session = store.OpenSession())
                {
                    var result = session.Query<Order>(indexName).ToList();
                    Assert.Equal(1, result.Count);
                }
                var stats = store.DatabaseCommands.GetStatistics();
                Assert.Equal(1, stats.CountOfDocuments);

                store.DatabaseCommands.Admin.DisableIndex(indexName);

                using (var session = store.OpenSession())
                {
                    session.Delete("orders/1");
                    session.SaveChanges();
                }

                WaitForIndexing(store);
                stats = store.DatabaseCommands.GetStatistics();
                Assert.Equal(0, stats.CountOfDocuments);

                var testIndex = store.DatabaseCommands.GetIndexStatistics(indexName);
                Assert.Equal(1, testIndex.EntriesCount);
            }
        }

        private class Order
        {
            public string Name { get; set; }
        }
    }
}