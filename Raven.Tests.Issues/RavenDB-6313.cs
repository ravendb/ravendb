// -----------------------------------------------------------------------
//  <copyright file="RavenDB-1279.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Issues
{
    public class RavenDB_6313 : RavenTest
    {
        protected override void ModifyConfiguration(Database.Config.InMemoryRavenConfiguration configuration)
        {
            configuration.Settings["Raven/NumberOfItemsToExecuteReduceInSingleStep"] = "256";
            configuration.Settings["Raven/MaxNumberOfItemsToReduceInSingleBatch"] = "256";
        }

        [Theory]
        [InlineData("voron", 1)]
        [InlineData("voron", 3)]
        [InlineData("voron", 1023)]
        [InlineData("voron", 1024)]
        [InlineData("voron", 3 * 1024)]
        [InlineData("voron", 10 * 1024)]
        [InlineData("esent", 1)]
        [InlineData("esent", 3)]
        [InlineData("esent", 1023)]
        [InlineData("esent", 1024)]
        [InlineData("esent", 3 * 1024)]
        [InlineData("esent", 10 * 1024)]
        public void can_finish_reduce(string storageName, int batchSize)
        {
            using (var store = NewRemoteDocumentStore(requestedStorage: storageName))
            {
                using (var bulk = store.BulkInsert())
                {
                    for (var i = 0; i < batchSize; i++)
                        bulk.Store(new Order {CompanyName = "Hibernating Rhinos"});
                }

                new MapReduceIndex().Execute(store);

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var list = session.Query<MapReduceIndex.Result, MapReduceIndex>().ToList();
                    Assert.Equal(1, list.Count);
                    Assert.Equal(batchSize, list[0].Count);
                }
            }
        }

        public class Order
        {
            public string Id { get; set; }
            public string CompanyName { get; set; }
        }

        private class MapReduceIndex : AbstractIndexCreationTask<Order, MapReduceIndex.Result>
        {
            public class Result
            {
                public string CompanyName { get; set; }

                public int Count { get; set; }
            }

            public MapReduceIndex()
            {
                Map = orders => from order in orders
                                select new
                                {
                                    CompanyName = order.CompanyName,
                                    Count = 1
                                };

                Reduce = results => from result in results
                                    group result by result.CompanyName into g
                                    select new
                                    {
                                        CompanyName = g.Key,
                                        Count = g.Sum(x => x.Count)
                                    };
            }
        }
    }
}
