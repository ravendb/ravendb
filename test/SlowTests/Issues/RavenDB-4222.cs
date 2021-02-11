// -----------------------------------------------------------------------
//  <copyright file="RavenDB-4222.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_4222 : RavenTestBase
    {
        public RavenDB_4222(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void DontUpdateDisabledIndex()
        {
            using (var store = GetDocumentStore())
            {
                var indexName = "test";
                store.Maintenance.Send(new PutIndexesOperation(new[] { new IndexDefinition
                {
                    Name = indexName,
                    Maps = { @"from doc in docs.Orders
select new{
doc.Name
}" }
                }}));

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
                var stats = store.Maintenance.Send(new GetStatisticsOperation());
                Assert.Equal(1, stats.CountOfDocuments);

                store.Maintenance.Send(new DisableIndexOperation(indexName));

                using (var session = store.OpenSession())
                {
                    session.Delete("orders/1");
                    session.SaveChanges();
                }

                WaitForIndexing(store);
                stats = store.Maintenance.Send(new GetStatisticsOperation());
                Assert.Equal(0, stats.CountOfDocuments);

                Assert.Equal(1, WaitForEntriesCount(store, indexName, 1));
            }
        }

        private class Order
        {
            public string Name { get; set; }
        }
    }
}
