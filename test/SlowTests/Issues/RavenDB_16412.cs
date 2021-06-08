using System;
using System.Collections.Generic;
using FastTests;
using FastTests.Server.Basic.Entities;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16412 : RavenTestBase
    {
        public RavenDB_16412(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void ShouldIndexAllDocumentsWithMaxNumberOfConcurrentlyRunningIndexesSet()
        {
            using (var server = GetNewServer(new ServerCreationOptions()
            {
                CustomSettings = new Dictionary<string, string>
                {
                    [RavenConfiguration.GetKey(x => x.Indexing.MaxNumberOfConcurrentlyRunningIndexes)] = "2",
                    [RavenConfiguration.GetKey(x => x.Indexing.MapBatchSize)] = "128" // to make sure each index will need multiple batches
                }
            }))
            using (var store = GetDocumentStore(new Options
            {
                Server = server,
            }))
            {
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 1024; i++)
                    {
                        session.Store(new Order
                        {
                            OrderedAt = DateTime.UtcNow,
                            ShippedAt = DateTime.UtcNow.AddDays(1)
                        });
                    }

                    session.SaveChanges();
                }

                for (int i = 0; i < 20; i++)
                {
                    new Orders_ByOrderedAtAndShippedAt("index_" + i).Execute(store);
                }

                WaitForIndexing(store, allowErrors: false);

                WaitForUserToContinueTheTest(store);
            }
        }

        private class Orders_ByOrderedAtAndShippedAt : AbstractIndexCreationTask
        {
            private readonly string _name;

            public Orders_ByOrderedAtAndShippedAt(string name)
            {
                _name = name;
            }

            public override string IndexName => _name;

            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition()
                {
                    Maps =
                    {
                        @"from o in docs.Orders
                        select new
                        {
                            o.OrderedAt,
                            o.ShippedAt
                        }"
                    },
                };
            }
        }
    }
}
