using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12151 : RavenTestBase
    {
        [Fact]
        public void IndexingWhenTransactionSizeLimitExceeded()
        {
            using (var store = GetDocumentStore(new Options()
            {
                ModifyDatabaseRecord = r =>
                {
                    r.Settings[RavenConfiguration.GetKey(x => x.Indexing.TransactionSizeLimit)] = "16";
                }
            }))
            {
                RunTest(store, "Reached transaction size limit");
            }
        }

        [Fact]
        public void IndexingWhenScratchSpaceLimitExceeded()
        {
            using (var store = GetDocumentStore(new Options()
            {
                Path = NewDataPath(),
                ModifyDatabaseRecord = r =>
                {
                    r.Settings[RavenConfiguration.GetKey(x => x.Storage.MaxScratchBufferSize)] = "2";
                    r.Settings[RavenConfiguration.GetKey(x => x.Indexing.ScratchSpaceLimit)] = "32";
                }
            }))
            {
                RunTest(store, "Reached scratch space limit");
            }
        }

        [Fact]
        public void IndexingWhenGlobalScratchSpaceLimitExceeded()
        {
            UseNewLocalServer(new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Indexing.GlobalScratchSpaceLimit)] = "32"
            });

            using (var store = GetDocumentStore(new Options()
            {
                Path = NewDataPath(),
                ModifyDatabaseRecord = r => r.Settings[RavenConfiguration.GetKey(x => x.Storage.MaxScratchBufferSize)] = "2"
            }))
            {
                RunTest(store, "Reached global scratch space limit");
            }
        }

        private void RunTest(DocumentStore store, string endOfPatchReason)
        {
            using (var bulk = store.BulkInsert())
            {
                for (int i = 0; i < 2000; i++)
                {
                    bulk.Store(new Order()
                    {
                        Company = $"companies/{i}",
                        Employee = $"employee/{i}",
                        Lines = new List<OrderLine>()
                        {
                            new OrderLine()
                            {
                                Product = $"products/{i}",
                                ProductName = new string((char)i, i)
                            },
                            new OrderLine()
                            {
                                Product = $"products/{i}",
                                ProductName = new string((char)i, i)
                            },
                        }
                    });
                }
            }

            SimpleIndex index = new SimpleIndex();

            index.Execute(store);

            var indexInstance = GetDatabase(store.Database).Result.IndexStore.GetIndex(index.IndexName);

            indexInstance._indexStorage.Environment().Options.MaxNumberOfPagesInJournalBeforeFlush = 4;

            using (var session = store.OpenSession())
            {
                var count = session.Query<Order, SimpleIndex>().Customize(x => x.WaitForNonStaleResults(TimeSpan.FromMinutes(2))).Count();

                Assert.Equal(4000, count);
            }

            var stats = indexInstance.GetIndexingPerformance();

            var mapRunDetails = stats.Select(x => x.Details.Operations.Select(y => y.MapDetails)).SelectMany(x => x).Where(x => x != null).ToList();

            Assert.True(mapRunDetails.Any(x => x.BatchCompleteReason.Contains(endOfPatchReason)));
        }

        public class SimpleIndex : AbstractIndexCreationTask<Order>
        {
            public SimpleIndex()
            {
                Map = orders => from order in orders
                    from item in order.Lines
                    select new
                    {
                        order.Company,
                        order.Employee,
                        item.Product,
                        item.ProductName
                    };
            }
        }
    }
}
