using System;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.Indexing.MapReduce
{
    public class OutputReduceToCollectionTests : RavenTestBase
    {
        public OutputReduceToCollectionTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task ReduceResultsBackAsDocuments()
        {
            using (var store = GetDocumentStore())
            {
                await CreateDataAndIndexes(store);

                var collectionStats = await store.Maintenance.SendAsync(new GetCollectionStatisticsOperation());
                Assert.Equal(120, collectionStats.Collections["Invoices"]);
                Assert.Equal(93, collectionStats.Collections["DailyInvoices"]);
                Assert.Equal(31, collectionStats.Collections["MonthlyInvoices"]);
                Assert.Equal(4, collectionStats.Collections["YearlyInvoices"]);

                using (var session = store.OpenAsyncSession())
                {
                    Assert.Equal(120, await session.Query<Invoice>().CountAsync());
                    Assert.Equal(93, await session.Query<DailyInvoice>().CountAsync());
                    Assert.Equal(31, await session.Query<MonthlyInvoice>().CountAsync());
                    Assert.Equal(4, await session.Query<YearlyInvoice>().CountAsync());
                }
            }
        }

        [Fact]
        public async Task ForbidOutputReduceDocumentsOnTheDocumentsWeMap()
        {
            using (var store = GetDocumentStore())
            {
                var exception = await Assert.ThrowsAsync<IndexInvalidException>(async () => await store.ExecuteIndexAsync(new MonthlySelfReduceIndex()));
                Assert.Contains("It is forbidden to create the 'MonthlySelfReduceIndex' index " +
                                "which would output reduce results to documents in the 'DailyInvoices' collection, " +
                                "as this index is mapping or referencing the 'DailyInvoices' collection and this will result in an infinite loop.", exception.Message);
            }
        }

        [Fact]
        public async Task ForbidOutputReduceDocumentsOnTheDocumentsWeLoadInMap()
        {
            using (var store = GetDocumentStore())
            {
                var exception = await Assert.ThrowsAsync<IndexInvalidException>(async () => await store.ExecuteIndexAsync(new InvoiceSelfReduceLoadDocumentIndex()));
                Assert.Contains("It is forbidden to create the 'InvoiceSelfReduceLoadDocumentIndex' index " +
                                "which would output reduce results to documents in the 'Invoices' collection, " +
                                "as this index is mapping or referencing the 'Invoices' collection and this will result in an infinite loop.", exception.Message);
            }
        }

        [Fact]
        public async Task ForbidOutputReduceDocumentsInAInfiniteLoop()
        {
            using (var store = GetDocumentStore())
            {
                await store.ExecuteIndexAsync(new DailyInvoicesIndex());
                await store.ExecuteIndexAsync(new MonthlyInvoicesIndex());
                var exception = await Assert.ThrowsAsync<IndexInvalidException>(async () => await store.ExecuteIndexAsync(new YearlyToDailyInfiniteLoopIndex()));
                Assert.Contains($"DailyInvoicesIndex: Invoices => DailyInvoices{Environment.NewLine}" +
                                $"MonthlyInvoicesIndex: DailyInvoices => MonthlyInvoices{Environment.NewLine}" +
                                $"--> YearlyToDailyInfiniteLoopIndex: MonthlyInvoices => *Invoices*", exception.Message);
            }
        }

        [Fact]
        public async Task ForbidOutputReduceDocumentsInAInfiniteLoopCausedByLoadDocument()
        {
            using (var store = GetDocumentStore())
            {
                await store.ExecuteIndexAsync(new DailyInvoicesIndexLoadDocument());
                await store.ExecuteIndexAsync(new MonthlyInvoicesIndex());
                var exception = await Assert.ThrowsAsync<IndexInvalidException>(async () => await store.ExecuteIndexAsync(new YearlyToDailyInfiniteLoopIndex()));
                Assert.Contains($"It is forbidden to create the 'YearlyToDailyInfiniteLoopIndex' index " +
                                "which would output reduce results to documents in the 'Invoices' collection, " +
                                "as 'Invoices' collection is consumed by other index in a way that would lead to an infinite loop." +
                                $"{Environment.NewLine}" +
                                $"DailyInvoicesIndexLoadDocument: InvoiceHolders (referenced: Invoices) => DailyInvoices{Environment.NewLine}" +
                                $"MonthlyInvoicesIndex: DailyInvoices => MonthlyInvoices{Environment.NewLine}" +
                                $"--> YearlyToDailyInfiniteLoopIndex: MonthlyInvoices => *Invoices*", exception.Message);
            }
        }

        [Fact]
        public async Task ForbidOutputReduceDocumentsToExistingCollectionWhichHaveDocuments()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new DailyInvoice {Amount = 1, Date = new DateTime(2017, 1, 1)});
                    await session.SaveChangesAsync();
                }

                var exception = await Assert.ThrowsAsync<IndexInvalidException>(async () => await CreateDataAndIndexes(store));
                Assert.Contains("Index 'DailyInvoicesIndex' is defined to output the Reduce results to documents in Collection 'DailyInvoices'." +
                                " This collection currently has 1 document . All documents in Collection 'DailyInvoices' must be deleted first.", exception.Message);
            }
        }

        [Fact]
        public async Task LetTheUserModifyTheIndex()
        {
            using (var store = GetDocumentStore())
            {
                await CreateDataAndIndexes(store);

                await store.ExecuteIndexAsync(new Reset.DailyInvoicesIndex());
                store.Operations.Send(new DeleteByQueryOperation(new IndexQuery {Query = "FROM DailyInvoices"})).WaitForCompletion(TimeSpan.FromSeconds(60));
                // We need to wait for the cluster to update the index before overwriting the index again
                WaitForIndexing(store);

                await store.ExecuteIndexAsync(new Replacement_AverageFieldAdded.DailyInvoicesIndex());
                WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                using (var session = store.OpenAsyncSession())
                {
                    Assert.Equal(120, await session.Query<Invoice>().CountAsync());
                    Assert.Equal(93, await session.Query<DailyInvoice>().CountAsync());
                    Assert.Equal(31, await session.Query<MonthlyInvoice>().CountAsync());
                    Assert.Equal(4, await session.Query<YearlyInvoice>().CountAsync());
                }
            }
        }

        [Fact]
        public async Task CanResetIndex()
        {
            using (var store = GetDocumentStore())
            {
                await CreateDataAndIndexes(store);

                await store.Maintenance.SendAsync(new ResetIndexOperation(new DailyInvoicesIndex().IndexName));
                await store.Maintenance.SendAsync(new ResetIndexOperation(new MonthlyInvoicesIndex().IndexName));
                await store.Maintenance.SendAsync(new ResetIndexOperation(new YearlyInvoicesIndex().IndexName));

                WaitForIndexing(store);

                using (var session = store.OpenAsyncSession())
                {
                    Assert.Equal(120, await session.Query<Invoice>().CountAsync());
                    Assert.Equal(93, await session.Query<DailyInvoice>().CountAsync());
                    Assert.Equal(31, await session.Query<MonthlyInvoice>().CountAsync());
                    Assert.Equal(4, await session.Query<YearlyInvoice>().CountAsync());
                }
            }
        }

        [Fact]
        public async Task CanUpdateIndexAsSideBySide()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new DailyInvoicesIndex());

                var date = new DateTime(2017, 1, 1);

                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 0; i < 30; i++)
                    {
                        await session.StoreAsync(new Invoice {Amount = 1, IssuedAt = date.AddHours(i * 6)});
                    }

                    date = date.AddYears(1);

                    for (int i = 0; i < 30; i++)
                    {
                        await session.StoreAsync(new Invoice {Amount = 1, IssuedAt = date.AddMonths(i).AddHours(i * 6)});
                        await session.StoreAsync(new Invoice {Amount = 1, IssuedAt = date.AddMonths(i).AddHours(i * 12)});
                        await session.StoreAsync(new Invoice {Amount = 1, IssuedAt = date.AddMonths(i).AddHours(i * 18)});
                    }

                    await session.SaveChangesAsync();
                }

                WaitForIndexing(store);
                await store.ExecuteIndexAsync(new Replacement_AverageFieldAdded.DailyInvoicesIndex());
                WaitForIndexing(store);

                using (var session = store.OpenAsyncSession())
                {
                    Assert.Equal(93, await session.Query<DailyInvoice>().CountAsync());
                }
            }
        }

        [Fact]
        public async Task CanUpdateIndexAsSideBySideAndChangingReduceOutputCollection()
        {
            using (var store = GetDocumentStore())
            {
                var index = new DailyInvoicesIndex();
                store.ExecuteIndex(index);

                var date = new DateTime(2017, 1, 1);

                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 0; i < 30; i++)
                    {
                        await session.StoreAsync(new Invoice { Amount = 1, IssuedAt = date.AddHours(i * 6) });
                    }

                    date = date.AddYears(1);

                    for (int i = 0; i < 30; i++)
                    {
                        await session.StoreAsync(new Invoice { Amount = 1, IssuedAt = date.AddMonths(i).AddHours(i * 6) });
                        await session.StoreAsync(new Invoice { Amount = 1, IssuedAt = date.AddMonths(i).AddHours(i * 12) });
                        await session.StoreAsync(new Invoice { Amount = 1, IssuedAt = date.AddMonths(i).AddHours(i * 18) });
                    }

                    await session.SaveChangesAsync();
                }

                WaitForIndexing(store);

                store.Maintenance.Send(new StopIndexingOperation());

                await store.ExecuteIndexAsync(new Replacement_DifferentOutputReduceToCollection.DailyInvoicesIndex());

                await store.ExecuteIndexAsync(new Replacement_DifferentOutputReduceToCollection2.DailyInvoicesIndex());

                var db = await GetDatabase(store.Database);

                var indexes = db.IndexStore.GetIndexes().ToList();

                var replacement = (MapReduceIndex)indexes.First(x => x.Name.StartsWith(Constants.Documents.Indexing.SideBySideIndexNamePrefix));

                // new replacement needs to delete docs created by original index
                var prefixesOfDocumentsToDelete = replacement.OutputReduceToCollection.GetPrefixesOfDocumentsToDelete().OrderBy(x => x.Key).ToList();

                Assert.Equal(2, prefixesOfDocumentsToDelete.Count);

                Assert.StartsWith("DailyInvoices/", prefixesOfDocumentsToDelete[0].Key);
                Assert.StartsWith("MyDailyInvoices/", prefixesOfDocumentsToDelete[1].Key);

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                WaitForUserToContinueTheTest(store);

                using (var session = store.OpenAsyncSession())
                {
                    Assert.Equal(0, await session.Query<DailyInvoice>().CountAsync());
                    Assert.Equal(0, await session.Advanced.AsyncRawQuery<object>("from MyDailyInvoices").CountAsync());
                    Assert.Equal(93, await session.Advanced.AsyncRawQuery<object>("from MyFavoriteDailyInvoices").CountAsync());
                }
            }
        }

        [Fact]
        public async Task CanEditExistingSideBySideIndex()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new DailyInvoicesIndex());

                var date = new DateTime(2017, 1, 1);

                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 0; i < 30; i++)
                    {
                        await session.StoreAsync(new Invoice {Amount = 1, IssuedAt = date.AddHours(i * 6)});
                    }

                    date = date.AddYears(1);

                    for (int i = 0; i < 30; i++)
                    {
                        await session.StoreAsync(new Invoice {Amount = 1, IssuedAt = date.AddMonths(i).AddHours(i * 6)});
                        await session.StoreAsync(new Invoice {Amount = 1, IssuedAt = date.AddMonths(i).AddHours(i * 12)});
                        await session.StoreAsync(new Invoice {Amount = 1, IssuedAt = date.AddMonths(i).AddHours(i * 18)});
                    }

                    await session.SaveChangesAsync();
                }

                WaitForIndexing(store);

                store.Maintenance.Send(new StopIndexingOperation());

                await store.ExecuteIndexAsync(new Replacement_AverageFieldAdded.DailyInvoicesIndex());

                await store.ExecuteIndexAsync(new Replacement_CountFieldAdded.DailyInvoicesIndex());

                var db = await GetDatabase(store.Database);

                var indexes = db.IndexStore.GetIndexes().ToList();

                var replacement = (MapReduceIndex)indexes.First(x => x.Name.StartsWith(Constants.Documents.Indexing.SideBySideIndexNamePrefix));

                // new replacement needs to delete docs created by original index
                Assert.Equal(2, replacement.OutputReduceToCollection.GetPrefixesOfDocumentsToDelete().Count);

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                using (var session = store.OpenAsyncSession())
                {
                    Assert.Equal(93, await session.Query<DailyInvoice>().CountAsync());

                    Assert.True((await session.Query<DailyInvoice>().FirstAsync()).Count > 0);
                }
            }
        }

        [Fact]
        public async Task CanChangeOutputReduceToCollection()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new DailyInvoicesIndex());

                var date = new DateTime(2017, 1, 1);

                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 0; i < 30; i++)
                    {
                        await session.StoreAsync(new Invoice {Amount = 1, IssuedAt = date.AddHours(i * 6)});
                    }

                    date = date.AddYears(1);

                    for (int i = 0; i < 30; i++)
                    {
                        await session.StoreAsync(new Invoice {Amount = 1, IssuedAt = date.AddMonths(i).AddHours(i * 6)});
                        await session.StoreAsync(new Invoice {Amount = 1, IssuedAt = date.AddMonths(i).AddHours(i * 12)});
                        await session.StoreAsync(new Invoice {Amount = 1, IssuedAt = date.AddMonths(i).AddHours(i * 18)});
                    }

                    await session.SaveChangesAsync();
                }

                WaitForIndexing(store);

                await store.ExecuteIndexAsync(new Replacement_OutputReduceToCollection_Changed.DailyInvoicesIndex());

                WaitForIndexing(store);

                using (var session = store.OpenAsyncSession())
                {
                    Assert.Equal(0, await session.Query<DailyInvoice>().CountAsync());
                    Assert.Equal(93, await session.Query<MyDailyInvoice>().CountAsync());
                }
            }
        }

        [Fact]
        public void MustNotAllowToDoSideBySideIfUsingLegacyIndexDefinitionWithoutReduceOutputIndex()
        {
            var backupPath = NewDataPath(forceCreateDir: true);
            var fullBackupPath = Path.Combine(backupPath, "ravendb-11488.ravendb-snapshot");

            ExtractFile(fullBackupPath, "RavenDB_11488.ravendb-11488.ravendb-snapshot");

            using (var store = GetDocumentStore())
            {
                var databaseName = GetDatabaseName();

                using (RestoreDatabase(store, new RestoreBackupConfiguration {BackupLocation = backupPath, DatabaseName = databaseName}))
                {
                    var exception = Assert.Throws<IndexInvalidException>(() => new Replacement_FieldNamesChanged.Orders_ByCompany().Execute(store, database: databaseName));

                    Assert.Contains("Index 'Orders/ByCompany' is defined to output the Reduce results to documents in Collection 'OrdersByCompany'. " +
                                    "This collection currently has 89 documents. All documents in Collection 'OrdersByCompany' must be deleted first.", exception.Message);
                }
            }
        }

        [Fact]
        public async Task WillProduceOutputResultsDocsAfterIndexImport()
        {
            using (var store = GetDocumentStore())
            {
                using (var stream = GetDump("RavenDB_11488.ravendb-11488-artificial-docs-not-included.ravendbdump"))
                {
                    var operation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), stream);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                }

                WaitForIndexing(store);

                using (var session = store.OpenAsyncSession())
                {
                    Assert.Equal(89, await session.Query<OrderByCompany>(collectionName: "OrdersByCompany").CountAsync());

                    var doc = await session.Query<OrderByCompany>(collectionName: "OrdersByCompany").FirstAsync();

                    var db = await GetDatabase(store.Database);

                    var indexes = db.IndexStore.GetIndexes().ToList();

                    var index = (MapReduceIndex)indexes.First(x => x.Name == "Orders/ByCompany");

                    Assert.StartsWith($"OrdersByCompany/{index.Definition.ReduceOutputIndex}/", doc.Id);
                }
            }
        }

        [Fact]
        public async Task WhenDeletingSideBySideIndexTheOriginalOneWillDeleteItsDocuments()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new DailyInvoicesIndex());

                var date = new DateTime(2017, 1, 1);

                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 0; i < 30; i++)
                    {
                        await session.StoreAsync(new Invoice { Amount = 1, IssuedAt = date.AddHours(i * 6) });
                    }

                    date = date.AddYears(1);

                    for (int i = 0; i < 30; i++)
                    {
                        await session.StoreAsync(new Invoice { Amount = 1, IssuedAt = date.AddMonths(i).AddHours(i * 6) });
                        await session.StoreAsync(new Invoice { Amount = 1, IssuedAt = date.AddMonths(i).AddHours(i * 12) });
                        await session.StoreAsync(new Invoice { Amount = 1, IssuedAt = date.AddMonths(i).AddHours(i * 18) });
                    }

                    await session.SaveChangesAsync();
                }

                WaitForIndexing(store);

                store.Maintenance.Send(new StopIndexingOperation());

                await store.ExecuteIndexAsync(new Replacement_DifferentOutputReduceToCollection.DailyInvoicesIndex());

                var db = await GetDatabase(store.Database);

                var replacementIndex = (MapReduceIndex)db.IndexStore.GetIndexes().Single(x => x.Name.StartsWith(Constants.Documents.Indexing.SideBySideIndexNamePrefix));
                var replacementIndexReduceOutputIndex = replacementIndex.Definition.ReduceOutputIndex.Value;

                store.Maintenance.Send(new DeleteIndexOperation("ReplacementOf/DailyInvoicesIndex"));

                var indexes = db.IndexStore.GetIndexes().ToList();

                var originalIndex = (MapReduceIndex)indexes.Single();

                // original index needs to delete docs created by replacement index

                Assert.Equal(1, originalIndex.OutputReduceToCollection.GetPrefixesOfDocumentsToDelete().Count);
                Assert.Equal($"MyDailyInvoices/{replacementIndexReduceOutputIndex}/", originalIndex.OutputReduceToCollection.GetPrefixesOfDocumentsToDelete().First().Key);


                using (var session = store.OpenAsyncSession())
                {
                    Assert.Equal(93, await session.Query<DailyInvoice>().CountAsync());
                }
            }
        }

        [Fact]
        public async Task UnchangedResultDoesntWriteDocuments()
        {
            using (var store = GetDocumentStore())
            {
                var date = new DateTime(1985, 8, 13);

                using (var session = store.OpenAsyncSession())
                {
                    for (var i = 0; i < 10; i++)
                    {
                        await session.StoreAsync(new Invoice
                        {
                            Amount = 1,
                            IssuedAt = date.AddDays(i * 1)
                        });
                    }

                    await session.SaveChangesAsync();
                }

                await store.ExecuteIndexAsync(new DailyInvoicesIndex());

                WaitForIndexing(store);
                await AssertNumberOfResults();
                string lastChangeVector;

                using (var session = store.OpenAsyncSession())
                {
                    // the reduce result shouldn't change
                    Invoice invoice = null;
                    for (var i = 0; i < 10; i++)
                    {
                        invoice = new Invoice
                        {
                            Amount = 0,
                            IssuedAt = date
                        };
                        await session.StoreAsync(invoice);
                    }

                    await session.SaveChangesAsync();

                    lastChangeVector = session.Advanced.GetChangeVectorFor(invoice);
                }

                WaitForIndexing(store);
                await AssertNumberOfResults();

                var newChangeVector = store.Maintenance.Send(new GetStatisticsOperation()).DatabaseChangeVector;
                Assert.Equal(lastChangeVector, newChangeVector);

                async Task AssertNumberOfResults()
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        Assert.Equal(10, await session.Query<DailyInvoice, DailyInvoicesIndex>().CountAsync());
                    }
                }
            }
        }

        private static Stream GetDump(string name)
        {
            var assembly = typeof(OutputReduceToCollectionTests).Assembly;
            return assembly.GetManifestResourceStream("SlowTests.Data." + name);
        }


        private static void ExtractFile(string extractPath, string name)
        {
            var assembly = typeof(OutputReduceToCollectionTests).Assembly;

            using (var file = File.Create(extractPath))
            using (var stream = assembly.GetManifestResourceStream("SlowTests.Data." + name))
            {
                stream.CopyTo(file);
            }
        }

        private async Task CreateDataAndIndexes(DocumentStore store)
        {
            var date = new DateTime(2017, 1, 1);

            using (var session = store.OpenAsyncSession())
            {
                for (int i = 0; i < 30; i++)
                {
                    await session.StoreAsync(new Invoice { Amount = 1, IssuedAt = date.AddHours(i * 6) });
                }
                date = date.AddYears(1);
                for (int i = 0; i < 30; i++)
                {
                    await session.StoreAsync(new Invoice { Amount = 1, IssuedAt = date.AddMonths(i).AddHours(i * 6) });
                    await session.StoreAsync(new Invoice { Amount = 1, IssuedAt = date.AddMonths(i).AddHours(i * 12) });
                    await session.StoreAsync(new Invoice { Amount = 1, IssuedAt = date.AddMonths(i).AddHours(i * 18) });
                }
                await session.SaveChangesAsync();
            }

            await store.ExecuteIndexAsync(new DailyInvoicesIndex());
            await store.ExecuteIndexAsync(new MonthlyInvoicesIndex());
            await store.ExecuteIndexAsync(new YearlyInvoicesIndex());

            WaitForIndexing(store);

            WaitForValue(() =>
            {
                var stats = store.Maintenance.Send(new GetCollectionStatisticsOperation());
                return stats.Collections != null && stats.Collections.Count >= 5;
            }, true);

            RavenTestHelper.AssertNoIndexErrors(store);
        }

        public class Marker
        {
            public string Name { get; set; }
        }

        public class Invoice
        {
            public string Id { get; set; }
            public decimal Amount { get; set; }
            public DateTime IssuedAt { get; set; }
        }

        public class InvoiceHolder
        {
            public string Id { get; set; }
            public string InvoiceId { get; set; }
        }

        public class DailyInvoice
        {
            public DateTime Date { get; set; }
            public decimal Amount { get; set; }
            public decimal Average { get; set; }
            public decimal Count { get; set; }
        }

        public class MyDailyInvoice
        {
            public DateTime Date { get; set; }
            public decimal Amount { get; set; }
        }

        public class MonthlyInvoice
        {
            public DateTime Date { get; set; }
            public decimal Amount { get; set; }
        }

        public class YearlyInvoice
        {
            public DateTime Date { get; set; }
            public decimal Amount { get; set; }
        }

        private class OrderByCompany
        {
            public string Id { get; set; }
        }

        public class DailyInvoicesIndex : AbstractIndexCreationTask<Invoice, DailyInvoice>
        {
            public DailyInvoicesIndex()
            {
                Map = invoices =>
                    from invoice in invoices
                    select new DailyInvoice
                    {
                        Date = invoice.IssuedAt.Date,
                        Amount = invoice.Amount
                    };

                Reduce = results =>
                    from r in results
                    group r by r.Date
                    into g
                    select new DailyInvoice
                    {
                        Date = g.Key,
                        Amount = g.Sum(x => x.Amount)
                    };

                OutputReduceToCollection = "DailyInvoices";
            }
        }

        public class DailyInvoicesIndexLoadDocument : AbstractIndexCreationTask<InvoiceHolder, DailyInvoice>
        {
            public DailyInvoicesIndexLoadDocument()
            {
                Map = invoiceHolders =>
                    from invoiceHolder in invoiceHolders
                    let invoice = LoadDocument<Invoice>(invoiceHolder.InvoiceId)
                    select new DailyInvoice
                    {
                        Date = invoice.IssuedAt.Date,
                        Amount = invoice.Amount
                    };

                Reduce = results =>
                    from r in results
                    group r by r.Date
                    into g
                    select new DailyInvoice
                    {
                        Date = g.Key,
                        Amount = g.Sum(x => x.Amount)
                    };

                OutputReduceToCollection = "DailyInvoices";
            }
        }

        private class MonthlyInvoicesIndex : AbstractIndexCreationTask<DailyInvoice, MonthlyInvoice>
        {
            public MonthlyInvoicesIndex()
            {
                Map = invoices =>
                    from invoice in invoices
                    select new MonthlyInvoice
                    {
                        Date = new DateTime(invoice.Date.Year, invoice.Date.Month, 1),
                        Amount = invoice.Amount
                    };

                Reduce = results =>
                    from r in results
                    group r by r.Date
                    into g
                    select new MonthlyInvoice
                    {
                        Date = g.Key,
                        Amount = g.Sum(x => x.Amount)
                    };

                OutputReduceToCollection = "MonthlyInvoices";
            }
        }

        private class YearlyInvoicesIndex : AbstractIndexCreationTask<MonthlyInvoice, YearlyInvoice>
        {
            public YearlyInvoicesIndex()
            {
                Map = invoices =>
                    from invoice in invoices
                    select new YearlyInvoice
                    {
                        Date = new DateTime(invoice.Date.Year, 1, 1),
                        Amount = invoice.Amount
                    };

                Reduce = results =>
                    from r in results
                    group r by r.Date
                    into g
                    select new YearlyInvoice
                    {
                        Date = g.Key,
                        Amount = g.Sum(x => x.Amount)
                    };

                OutputReduceToCollection = "YearlyInvoices";
            }
        }

        private class MonthlySelfReduceIndex : AbstractIndexCreationTask<DailyInvoice, DailyInvoice>
        {
            public MonthlySelfReduceIndex()
            {
                Map = invoices =>
                    from invoice in invoices
                    select new MonthlyInvoice
                    {
                        Date = new DateTime(invoice.Date.Year, invoice.Date.Month, 1),
                        Amount = invoice.Amount
                    };

                Reduce = results =>
                    from r in results
                    group r by r.Date
                    into g
                    select new MonthlyInvoice
                    {
                        Date = g.Key,
                        Amount = g.Sum(x => x.Amount)
                    };

                OutputReduceToCollection = "DailyInvoices";
            }
        }

        private class InvoiceSelfReduceLoadDocumentIndex : AbstractIndexCreationTask<InvoiceHolder, Invoice>
        {
            public InvoiceSelfReduceLoadDocumentIndex()
            {
                Map = invoiceHolders =>
                    from invoiceHolder in invoiceHolders
                    let invoice = LoadDocument<Invoice>(invoiceHolder.InvoiceId)
                    select new Invoice
                    {
                        IssuedAt = invoice.IssuedAt,
                        Amount = invoice.Amount
                    };

                Reduce = results =>
                    from r in results
                    group r by r.IssuedAt
                    into g
                    select new Invoice
                    {
                        IssuedAt = g.Key,
                        Amount = g.Sum(x => x.Amount)
                    };

                OutputReduceToCollection = "Invoices";
            }
        }

        private class YearlyToDailyInfiniteLoopIndex : AbstractIndexCreationTask<MonthlyInvoice, Invoice>
        {
            public YearlyToDailyInfiniteLoopIndex()
            {
                Map = invoices =>
                    from invoice in invoices
                    select new Invoice
                    {
                        IssuedAt = new DateTime(invoice.Date.Year, 1, 1),
                        Amount = invoice.Amount
                    };

                Reduce = results =>
                    from r in results
                    group r by r.IssuedAt
                    into g
                    select new Invoice
                    {
                        IssuedAt = g.Key,
                        Amount = g.Sum(x => x.Amount)
                    };

                OutputReduceToCollection = "Invoices";
            }
        }

        private static class Reset
        {
            public class DailyInvoicesIndex : AbstractIndexCreationTask<Invoice, DailyInvoice>
            {
                public DailyInvoicesIndex()
                {
                    Map = invoices =>
                        from invoice in invoices
                        select new DailyInvoice
                        {
                            Date = invoice.IssuedAt.Date,
                            Amount = invoice.Amount,
                        };

                    Reduce = results =>
                        from r in results
                        group r by r.Date
                        into g
                        select new DailyInvoice
                        {
                            Date = g.Key,
                            Amount = g.Sum(x => x.Amount),
                        };

                    OutputReduceToCollection = null;
                }
            }
        }

        private static class Replacement_AverageFieldAdded
        {
            public class DailyInvoicesIndex : AbstractIndexCreationTask<Invoice, DailyInvoice>
            {
                public DailyInvoicesIndex()
                {
                    Map = invoices =>
                        from invoice in invoices
                        select new DailyInvoice
                        {
                            Date = invoice.IssuedAt.Date,
                            Amount = invoice.Amount,
                            Average = invoice.Amount,
                        };

                    Reduce = results =>
                        from r in results
                        group r by r.Date
                        into g
                        select new DailyInvoice
                        {
                            Date = g.Key,
                            Amount = g.Sum(x => x.Amount),
                            Average = g.Average(x => x.Amount)
                        };

                    OutputReduceToCollection = "DailyInvoices";
                }
            }
        }

        private static class Replacement_CountFieldAdded
        {
            public class DailyInvoicesIndex : AbstractIndexCreationTask<Invoice, DailyInvoice>
            {
                public DailyInvoicesIndex()
                {
                    Map = invoices =>
                        from invoice in invoices
                        select new DailyInvoice
                        {
                            Date = invoice.IssuedAt.Date,
                            Amount = invoice.Amount,
                            Count = 1
                        };

                    Reduce = results =>
                        from r in results
                        group r by r.Date
                        into g
                        select new DailyInvoice
                        {
                            Date = g.Key,
                            Amount = g.Sum(x => x.Amount),
                            Count = g.Sum(x => x.Count)
                        };

                    OutputReduceToCollection = "DailyInvoices";
                }
            }
        }

        private static class Replacement_OutputReduceToCollection_Changed
        {
            public class DailyInvoicesIndex : AbstractIndexCreationTask<Invoice, MyDailyInvoice>
            {
                public DailyInvoicesIndex()
                {
                    Map = invoices =>
                        from invoice in invoices
                        select new MyDailyInvoice
                        {
                            Date = invoice.IssuedAt.Date,
                            Amount = invoice.Amount,
                        };

                    Reduce = results =>
                        from r in results
                        group r by r.Date
                        into g
                        select new MyDailyInvoice
                        {
                            Date = g.Key,
                            Amount = g.Sum(x => x.Amount),
                        };

                    OutputReduceToCollection = "MyDailyInvoices";
                }
            }
        }

        private static class Replacement_FieldNamesChanged
        {
            public class Orders_ByCompany : AbstractIndexCreationTask<Order, Orders_ByCompany.Result>
            {
                public class Result
                {
                    public string Company2 { get; set; }
                    public int Count2 { get; set; }
                    public decimal Total2 { get; set; }
                }

                public Orders_ByCompany()
                {
                    Map = orders => from order in orders
                        select new Result { Company2 = order.Company, Count2 = 1, Total2 = order.Lines.Sum(l => (l.Quantity * l.PricePerUnit) * (1 - l.Discount)) };

                    Reduce = results => from result in results
                        group result by result.Company2
                        into g
                        select new { Company2 = g.Key, Count2 = g.Sum(x => x.Count2), Total2 = g.Sum(x => x.Total2) };

                    OutputReduceToCollection = "OrdersByCompany";
                }
            }
        }

        private static class Replacement_DifferentOutputReduceToCollection
        {
            public class DailyInvoicesIndex : AbstractIndexCreationTask<Invoice, DailyInvoice>
            {
                public DailyInvoicesIndex()
                {
                    Map = invoices =>
                        from invoice in invoices
                        select new DailyInvoice
                        {
                            Date = invoice.IssuedAt.Date,
                            Amount = invoice.Amount
                        };

                    Reduce = results =>
                        from r in results
                        group r by r.Date
                        into g
                        select new DailyInvoice
                        {
                            Date = g.Key,
                            Amount = g.Sum(x => x.Amount)
                        };

                    OutputReduceToCollection = "MyDailyInvoices";
                }
            }
        }

        private static class Replacement_DifferentOutputReduceToCollection2
        {
            public class DailyInvoicesIndex : AbstractIndexCreationTask<Invoice, DailyInvoice>
            {
                public DailyInvoicesIndex()
                {
                    Map = invoices =>
                        from invoice in invoices
                        select new DailyInvoice
                        {
                            Date = invoice.IssuedAt.Date,
                            Amount = invoice.Amount
                        };

                    Reduce = results =>
                        from r in results
                        group r by r.Date
                        into g
                        select new DailyInvoice
                        {
                            Date = g.Key,
                            Amount = g.Sum(x => x.Amount)
                        };

                    OutputReduceToCollection = "MyFavoriteDailyInvoices";
                }
            }
        }
    }
}
