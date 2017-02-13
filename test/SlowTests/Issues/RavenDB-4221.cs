// -----------------------------------------------------------------------
//  <copyright file="RavenDB-4221.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Raven.Client.Indexing;
using Raven.Client.Operations.Databases;
using Raven.Client.Operations.Databases.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_4221 : RavenNewTestBase
    {
        [Fact]
        public void DisabledIndexIsUpdatedAfterSettingPriority()
        {
            using (var store = GetDocumentStore())
            {
                var indexName = "test";
                store.Admin.Send(new PutIndexesOperation(new [] { new IndexDefinition
                {
                    Name = indexName,
                    Maps = { @"from doc in docs.Orders
select new{
doc.Name
}" }
                }}));

                store.Admin.Send(new DisableIndexOperation(indexName));

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
                    Assert.Equal(0, result.Count);
                }

                var stats = store.Admin.Send(new GetStatisticsOperation());
                Assert.Equal(1, stats.CountOfDocuments);

                store.Admin.Send(new EnableIndexOperation(indexName));

                WaitForIndexing(store);
                using (var session = store.OpenSession())
                {
                    var result = session.Query<Order>(indexName).ToList();
                    Assert.Equal(1, result.Count);
                }

                stats = store.Admin.Send(new GetStatisticsOperation());
                Assert.Equal(1, stats.CountOfDocuments);
            }
        }

        private class Order
        {
            public string Name { get; set; }
        }
    }
}
