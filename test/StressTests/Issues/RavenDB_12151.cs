using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace StressTests.Issues
{
    public class RavenDB_12151 : RavenTestBase
    {
        public RavenDB_12151(ITestOutputHelper output) : base(output)
        {
        }

        [RavenMultiplatformFact(RavenTestCategory.Indexes, RavenArchitecture.AllX64)]
        public async Task IndexingWhenTransactionSizeLimitExceeded()
        {
            using (var store = GetDocumentStore(new Options()
            {
                ModifyDatabaseRecord = r =>
                {
                    r.Settings[RavenConfiguration.GetKey(x => x.Indexing.TransactionSizeLimit)] = "16";
                }
            }))
            {
                await RunTest(store, "Reached transaction size limit");
            }
        }

        [RavenMultiplatformFact(RavenTestCategory.Indexes, RavenArchitecture.AllX64)]
        public async Task IndexingWhenScratchSpaceLimitExceeded()
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
                await RunTest(store, "Reached scratch space limit");
            }
        }

        [RavenMultiplatformFact(RavenTestCategory.Indexes, RavenArchitecture.AllX64)]
        public async Task IndexingWhenGlobalScratchSpaceLimitExceeded()
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
                await RunTest(store, "Reached global scratch space limit");
            }
        }

        [RavenMultiplatformFact(RavenTestCategory.Indexes, RavenArchitecture.AllX86)]
        public async Task IndexingWhenTransactionSizeLimitExceeded32()
        {
            using (var store = GetDocumentStore(new Options()
            {
                ModifyDatabaseRecord = r =>
                {
                    r.Settings[RavenConfiguration.GetKey(x => x.Indexing.TransactionSizeLimit)] = "8";
                }
            }))
            {
                await RunTest32(store, "Reached transaction size limit");
            }
        }

        [RavenMultiplatformFact(RavenTestCategory.Indexes, RavenArchitecture.AllX86)]
        public async Task IndexingWhenScratchSpaceLimitExceeded32()
        {
            using (var store = GetDocumentStore(new Options()
            {
                Path = NewDataPath(),
                ModifyDatabaseRecord = r =>
                {
                    r.Settings[RavenConfiguration.GetKey(x => x.Storage.MaxScratchBufferSize)] = "2";
                    r.Settings[RavenConfiguration.GetKey(x => x.Indexing.ScratchSpaceLimit)] = "24";
                }
            }))
            {
                await RunTest32(store, "Reached scratch space limit");
            }
        }

        [RavenMultiplatformFact(RavenTestCategory.Indexes, RavenArchitecture.AllX86)]
        public async Task IndexingWhenGlobalScratchSpaceLimitExceeded32()
        {
            UseNewLocalServer(new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Indexing.GlobalScratchSpaceLimit)] = "24"
            });

            using (var store = GetDocumentStore(new Options()
            {
                Path = NewDataPath(),
                ModifyDatabaseRecord = r => r.Settings[RavenConfiguration.GetKey(x => x.Storage.MaxScratchBufferSize)] = "2"
            }))
            {
                await RunTest32(store, "Reached global scratch space limit");
            }
        }

        [Fact]
        public async Task IndexingWhenEncryptedTransactionSizeLimitLimitExceeded()
        {
            string dbName = Encryption.SetupEncryptedDatabase(out var certificates, out var _);

            using (var store = GetDocumentStore(new Options()
            {
                AdminCertificate = certificates.ServerCertificate.Value,
                ClientCertificate = certificates.ServerCertificate.Value,
                ModifyDatabaseName = s => dbName,
                Path = NewDataPath(),
                ModifyDatabaseRecord = r =>
                {
                    r.Settings[RavenConfiguration.GetKey(x => x.Indexing.EncryptedTransactionSizeLimit)] = "1";
                    r.Encrypted = true;
                }
            }))
            {
                await RunTest(store, "Reached transaction size limit");
            }
        }

        private async Task RunTest(DocumentStore store, string endOfPatchReason)
        {
            await using (var bulk = store.BulkInsert())
            {
                for (int i = 0; i < 2000; i++)
                {
                    await bulk.StoreAsync(new Order
                    {
                        Company = $"companies/{i}",
                        Employee = $"employee/{i}",
                        Lines = new List<OrderLine>
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

            await index.ExecuteAsync(store);

            var indexInstance = (await GetDatabase(store.Database)).IndexStore.GetIndex(index.IndexName);

            indexInstance._indexStorage.Environment().Options.MaxNumberOfPagesInJournalBeforeFlush = 4;

            var sw = Stopwatch.StartNew();

            try
            {
                using (var session = store.OpenAsyncSession())
                {
                    var count = await session.Query<Order, SimpleIndex>().Customize(x => x.WaitForNonStaleResults(TimeSpan.FromMinutes(5))).CountAsync();

                    Assert.Equal(4000, count);
                }
            }
            catch (Exception e)
            {
                sw.Stop();

                var sb = new StringBuilder();
                sb.AppendLine($"Ex: {e}")
                    .AppendLine($"Elapsed: {sw.Elapsed}");

                try
                {
                    await Indexes.WaitForIndexingAsync(store);
                }
                catch
                {
                }

                var errors = Indexes.WaitForIndexingErrors(store, errorsShouldExists: false);
                if (errors != null)
                {
                    foreach (var error in errors)
                    {
                        if (error != null && error.Errors != null && error.Errors.Length > 0)
                        {
                            sb.AppendLine($"Indexing errors for: '{error.Name}'");
                            foreach (var er in error.Errors)
                            {
                                sb.AppendLine($" - {er}");
                            }
                        }
                    }
                }

                throw new InvalidOperationException(sb.ToString());
            }

            var stats = indexInstance.GetIndexingPerformance();

            var mapRunDetails = stats.Select(x => x.Details.Operations.Select(y => y.MapDetails)).SelectMany(x => x).Where(x => x != null).ToList();

            Assert.True(mapRunDetails.Any(x => x.BatchCompleteReason.Contains(endOfPatchReason)));
        }

        private async Task RunTest32(DocumentStore store, string endOfPatchReason)
        {
            const int docsCount = 2500;
            await using (var bulk = store.BulkInsert())
            {
                for (int i = 0; i < docsCount; i++)
                {
                    await bulk.StoreAsync(new Order
                    {
                        Company = $"companies/{i}",
                        Employee = $"employee/{i}",
                        Lines = new List<OrderLine>
                        {
                            new OrderLine
                            {
                                Product = $"products/{i}",
                                ProductName = new string((char)i, i)
                            },
                            new OrderLine
                            {
                                Product = $"products/{i}",
                                ProductName = new string((char)i, i)
                            },
                        }
                    });
                }
            }

            var index = new SimpleIndex32();

            await index.ExecuteAsync(store);

            var indexInstance = (await GetDatabase(store.Database)).IndexStore.GetIndex(index.IndexName);

            indexInstance._indexStorage.Environment().Options.MaxNumberOfPagesInJournalBeforeFlush = 4;

            using (var session = store.OpenAsyncSession())
            {
                var count = await session.Query<Order, SimpleIndex32>().Customize(x => x.WaitForNonStaleResults(TimeSpan.FromMinutes(2))).CountAsync();

                Assert.Equal(docsCount, count);
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

        public class SimpleIndex32 : AbstractIndexCreationTask<Order, SimpleIndex32.Result>
        {
            public class Result
            {
                public string Company { get; set; }

                public string Employee { get; set; }

                public string Product { get; set; }

                public string ProductName { get; set; }

                public int Count { get; set; }

                public decimal Total { get; set; }
            }

            public SimpleIndex32()
            {
                Map = orders => from order in orders
                                from item in order.Lines
                                select new
                                {
                                    order.Company,
                                    order.Employee,
                                    item.Product,
                                    item.ProductName,
                                    Count = 1,
                                    Total = item.Quantity * item.PricePerUnit * (1 - item.Discount)
                                };

                Reduce = results => from result in results
                                    group result by new { result.Company, result.Employee, result.Product, result.ProductName } into g
                                    select new
                                    {
                                        g.Key.Company,
                                        g.Key.Employee,
                                        g.Key.Product,
                                        g.Key.ProductName,
                                        Count = g.Sum(x => x.Count),
                                        Total = g.Sum(x => x.Total)
                                    };
            }
        }
    }
}
