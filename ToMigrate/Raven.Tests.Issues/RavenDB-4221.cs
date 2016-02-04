// -----------------------------------------------------------------------
//  <copyright file="RavenDB-4221.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_4221 : RavenTest
    {
        [Fact]
        public void DisabledIndexIsUpdatedAfterSettingPriority()
        {
            using (var store = NewDocumentStore())
            {
                var indexName = "test";
                store.DatabaseCommands.PutIndex(indexName, new IndexDefinition
                {
                    Name = indexName,
                    Map = @"from doc in docs.Orders
select new{
doc.Name
}"
                });

                store.DatabaseCommands.SetIndexPriority(indexName, IndexingPriority.Disabled);

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

                var stats = store.DatabaseCommands.GetStatistics();
                Assert.Equal(1, stats.CountOfDocuments);

                store.DatabaseCommands.SetIndexPriority(indexName, IndexingPriority.Normal);

                WaitForIndexing(store);
                using (var session = store.OpenSession())
                {
                    var result = session.Query<Order>(indexName).ToList();
                    Assert.Equal(1, result.Count);
                }

                stats = store.DatabaseCommands.GetStatistics();
                Assert.Equal(1, stats.CountOfDocuments);
            }
        }

        private class Order
        {
            public string Name { get; set; }
        }
    }
}
