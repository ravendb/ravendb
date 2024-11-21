using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Counters;
using Raven.Client.Documents.Indexes.TimeSeries;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Session;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_23100 : RavenTestBase
    {
        public RavenDB_23100(ITestOutputHelper output) : base(output)
        {
        }

        private const string EmployeeId1 = "Em\tploy&es/1-A";
        private const string EmployeeId2 = "EmPl\ro#ees/2-A";
        private const string CompanyId = "COM\ban!es/1-A";

        private static readonly Company _company = new Company { Id = CompanyId, Name = "RavenDB" };
        private static readonly Employee _employee1 = new Employee { Id = EmployeeId1, CompanyId = _company.Id };
        private static readonly Employee _employee2 = new Employee { Id = EmployeeId2, CompanyId = _company.Id };

        private const string LegacyEmployeeId1 = "Employ&es/1-A";
        private const string LegacyEmployeeId2 = "EmPlo#ees/2-A";
        private const string LegacyCompanyId = "COMan!es/1-A";

        [RavenTheory(RavenTestCategory.Indexes)]
        [InlineData(typeof(DocumentsIndex))]
        [InlineData(typeof(DocumentsIndexMapReduce))]
        public async Task Can_Delete_Document_References(Type type)
        {
            using (var store = GetDocumentStore())
            {
                var index = (AbstractIndexCreationTask)Activator.CreateInstance(type);
                await store.ExecuteIndexAsync(index);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(_company);
                    await session.StoreAsync(_employee1);
                    await session.StoreAsync(_employee2);
                    await session.SaveChangesAsync();
                }

                Indexes.WaitForIndexing(store);

                var database = await GetDatabase(store.Database);
                var indexInstance = database.IndexStore.GetIndex(index.IndexName);

                using (indexInstance._contextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (var tx = context.OpenReadTransaction())
                {
                    var counts = indexInstance._indexStorage.ReferencesForDocuments.GetReferenceTablesCount("Employees", tx);

                    Assert.Equal(2, counts.ReferenceTableCount);
                    Assert.Equal(1, counts.CollectionTableCount);
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.Delete(_employee1.Id);
                    await session.SaveChangesAsync();
                }

                Indexes.WaitForIndexing(store);

                using (indexInstance._contextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (var tx = context.OpenReadTransaction())
                {
                    var counts = indexInstance._indexStorage.ReferencesForDocuments.GetReferenceTablesCount("Employees", tx);

                    Assert.Equal(1, counts.ReferenceTableCount);
                    Assert.Equal(1, counts.CollectionTableCount);
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.Delete(_employee2.Id);
                    await session.SaveChangesAsync();
                }

                Indexes.WaitForIndexing(store);

                using (indexInstance._contextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (var tx = context.OpenReadTransaction())
                {
                    var counts = indexInstance._indexStorage.ReferencesForDocuments.GetReferenceTablesCount("Employees", tx);

                    Assert.Equal(0, counts.ReferenceTableCount);
                    Assert.Equal(0, counts.CollectionTableCount);
                }
            }
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public async Task Can_Delete_Document_References_For_Legacy_Index()
        {
            var backupPath = NewDataPath(forceCreateDir: true);
            var fullBackupPath = Path.Combine(backupPath, "2024-11-18-17-16-52-7843589.ravendb-snapshot");

            await using (var file = File.Create(fullBackupPath))
            {
                await using (var stream = typeof(RavenDB_23100).Assembly.GetManifestResourceStream("SlowTests.Data.RavenDB_23100.2024-11-18-17-16-52-7843589.ravendb-snapshot"))
                {
                    await stream.CopyToAsync(file);
                }
            }

            using (var store = GetDocumentStore(new Options
            {
                CreateDatabase = false
            }))
            {
                using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration
                {
                    BackupLocation = backupPath,
                    DatabaseName = store.Database
                }))
                {
                    var database = await GetDatabase(store.Database);
                    var indexInstance = database.IndexStore.GetIndex(new DocumentsIndex().IndexName);

                    using (indexInstance._contextPool.AllocateOperationContext(out TransactionOperationContext context))
                    using (var tx = context.OpenReadTransaction())
                    {
                        var counts = indexInstance._indexStorage.ReferencesForDocuments.GetReferenceTablesCount("Employees", tx);

                        Assert.Equal(2, counts.ReferenceTableCount);
                        Assert.Equal(1, counts.CollectionTableCount);
                    }

                    using (var session = store.OpenAsyncSession())
                    {
                        var company = await session.LoadAsync<Company>(LegacyCompanyId);
                        company.Name += " LTD";
                        await session.SaveChangesAsync();
                    }

                    Indexes.WaitForIndexing(store);

                    using (var session = store.OpenAsyncSession())
                    {
                        var results = await session.Query<DocumentsIndex.Result, DocumentsIndex>()
                            .ProjectInto<DocumentsIndex.Result>().ToListAsync();
                        
                        Assert.Equal(2, results.Count);
                        
                        foreach (var company in results)
                        {
                            Assert.Equal("RavenDB LTD", company.CompanyName);
                        }
                    }

                    using (var session = store.OpenAsyncSession())
                    {
                        session.Delete(LegacyEmployeeId1);
                        session.Delete(LegacyEmployeeId2);
                        await session.SaveChangesAsync();
                        // deleting the documents won't change the internal references tree like in new indexes
                    }

                    Indexes.WaitForIndexing(store);

                    using (indexInstance._contextPool.AllocateOperationContext(out TransactionOperationContext context))
                    using (var tx = context.OpenReadTransaction())
                    {
                        var counts = indexInstance._indexStorage.ReferencesForDocuments.GetReferenceTablesCount("Employees", tx);

                        Assert.Equal(2, counts.ReferenceTableCount);
                        Assert.Equal(1, counts.CollectionTableCount);
                    }

                    using (var session = store.OpenAsyncSession())
                    {
                        var company = await session.LoadAsync<Company>(LegacyCompanyId);
                        company.Name += " HR";
                        await session.SaveChangesAsync();
                        // when we update the references, this will clean the leftovers
                    }

                    Indexes.WaitForIndexing(store);

                    using (indexInstance._contextPool.AllocateOperationContext(out TransactionOperationContext context))
                    using (var tx = context.OpenReadTransaction())
                    {
                        var counts = indexInstance._indexStorage.ReferencesForDocuments.GetReferenceTablesCount("Employees", tx);

                        Assert.Equal(0, counts.ReferenceTableCount);
                        Assert.Equal(0, counts.CollectionTableCount);
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public async Task Can_Delete_Document_References_Map_Reduce_For_Legacy_Index()
        {
            var backupPath = NewDataPath(forceCreateDir: true);
            var fullBackupPath = Path.Combine(backupPath, "2024-11-19-08-30-34-7519596.ravendb-snapshot");

            await using (var file = File.Create(fullBackupPath))
            {
                await using (var stream = typeof(RavenDB_23100).Assembly.GetManifestResourceStream("SlowTests.Data.RavenDB_23100.2024-11-19-08-30-34-7519596.ravendb-snapshot"))
                {
                    await stream.CopyToAsync(file);
                }
            }

            using (var store = GetDocumentStore(new Options
            {
                CreateDatabase = false
            }))
            {
                using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration
                {
                    BackupLocation = backupPath,
                    DatabaseName = store.Database
                }))
                {
                    var database = await GetDatabase(store.Database);
                    var indexInstance = database.IndexStore.GetIndex(new DocumentsIndexMapReduce().IndexName);

                    using (indexInstance._contextPool.AllocateOperationContext(out TransactionOperationContext context))
                    using (var tx = context.OpenReadTransaction())
                    {
                        var counts = indexInstance._indexStorage.ReferencesForDocuments.GetReferenceTablesCount("Employees", tx);

                        Assert.Equal(2, counts.ReferenceTableCount);
                        Assert.Equal(1, counts.CollectionTableCount);
                    }

                    using (var session = store.OpenAsyncSession())
                    {
                        var company = await session.LoadAsync<Company>(LegacyCompanyId);
                        company.Name += " LTD";
                        await session.SaveChangesAsync();
                    }

                    Indexes.WaitForIndexing(store);

                    using (var session = store.OpenAsyncSession())
                    {
                        var results = await session.Query<DocumentsIndexMapReduce.Result, DocumentsIndexMapReduce>()
                            .ProjectInto<DocumentsIndex.Result>().ToListAsync();

                        Assert.Equal(1, results.Count);
                        Assert.Equal("RavenDB LTD", results[0].CompanyName);
                    }

                    using (var session = store.OpenAsyncSession())
                    {
                        session.Delete(LegacyEmployeeId1);
                        session.Delete(LegacyEmployeeId2);
                        await session.SaveChangesAsync();
                        // deleting the documents won't change the internal references tree like in new indexes
                    }

                    Indexes.WaitForIndexing(store);

                    using (indexInstance._contextPool.AllocateOperationContext(out TransactionOperationContext context))
                    using (var tx = context.OpenReadTransaction())
                    {
                        var counts = indexInstance._indexStorage.ReferencesForDocuments.GetReferenceTablesCount("Employees", tx);

                        Assert.Equal(2, counts.ReferenceTableCount);
                        Assert.Equal(1, counts.CollectionTableCount);
                    }

                    using (var session = store.OpenAsyncSession())
                    {
                        var company = await session.LoadAsync<Company>(LegacyCompanyId);
                        company.Name += " HR";
                        await session.SaveChangesAsync();
                        // when we update the references, this will clean the leftovers
                    }

                    Indexes.WaitForIndexing(store);

                    using (indexInstance._contextPool.AllocateOperationContext(out TransactionOperationContext context))
                    using (var tx = context.OpenReadTransaction())
                    {
                        var counts = indexInstance._indexStorage.ReferencesForDocuments.GetReferenceTablesCount("Employees", tx);

                        Assert.Equal(0, counts.ReferenceTableCount);
                        Assert.Equal(0, counts.CollectionTableCount);
                    }
                }
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [InlineData(typeof(CompareExchangeIndex))]
        [InlineData(typeof(CompareExchangeMapReduceIndex))]
        public async Task Can_Delete_CompareExchange_References(Type type)
        {
            using (var store = GetDocumentStore())
            {
                var index = (AbstractIndexCreationTask)Activator.CreateInstance(type);

                await store.ExecuteIndexAsync(index);

                using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue(CompanyId, new Company { Name = "RavenDB" });
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenSession())
                {
                    session.Store(_employee1);
                    session.Store(_employee2);
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                var database = await GetDatabase(store.Database);
                var indexInstance = database.IndexStore.GetIndex(index.IndexName);

                using (indexInstance._contextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (var tx = context.OpenReadTransaction())
                {
                    var counts = indexInstance._indexStorage.ReferencesForCompareExchange.GetReferenceTablesCount("Employees", tx);

                    Assert.Equal(2, counts.ReferenceTableCount);
                    Assert.Equal(1, counts.CollectionTableCount);
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.Delete(_employee1.Id);
                    await session.SaveChangesAsync();
                }

                Indexes.WaitForIndexing(store);

                using (indexInstance._contextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (var tx = context.OpenReadTransaction())
                {
                    var counts = indexInstance._indexStorage.ReferencesForCompareExchange.GetReferenceTablesCount("Employees", tx);

                    Assert.Equal(1, counts.ReferenceTableCount);
                    Assert.Equal(1, counts.CollectionTableCount);
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.Delete(_employee2.Id);
                    await session.SaveChangesAsync();
                }

                Indexes.WaitForIndexing(store);

                using (indexInstance._contextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (var tx = context.OpenReadTransaction())
                {
                    var counts = indexInstance._indexStorage.ReferencesForCompareExchange.GetReferenceTablesCount("Employees", tx);

                    Assert.Equal(0, counts.ReferenceTableCount);
                    Assert.Equal(0, counts.CollectionTableCount);
                }

            }
        }

        [RavenFact(RavenTestCategory.Indexes, Skip = "https://issues.hibernatingrhinos.com/issue/RavenDB-23166/Snapshot-restore-of-with-compare-exchange-references")]
        public async Task Can_Delete_CompareExchange_References_For_Legacy_Index()
        {
            var backupPath = NewDataPath(forceCreateDir: true);
            var fullBackupPath = Path.Combine(backupPath, "2024-11-19-10-43-34-8245899.ravendb-snapshot");

            await using (var file = File.Create(fullBackupPath))
            {
                await using (var stream = typeof(RavenDB_23100).Assembly.GetManifestResourceStream("SlowTests.Data.RavenDB_23100.2024-11-19-10-43-34-8245899.ravendb-snapshot"))
                {
                    await stream.CopyToAsync(file);
                }
            }

            using (var store = GetDocumentStore(new Options
            {
                CreateDatabase = false
            }))
            {
                using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration
                {
                    BackupLocation = backupPath,
                    DatabaseName = store.Database
                }))
                {
                    var database = await GetDatabase(store.Database);
                    var indexInstance = database.IndexStore.GetIndex(new CompareExchangeIndex().IndexName);

                    using (indexInstance._contextPool.AllocateOperationContext(out TransactionOperationContext context))
                    using (var tx = context.OpenReadTransaction())
                    {
                        var counts = indexInstance._indexStorage.ReferencesForCompareExchange.GetReferenceTablesCount("Employees", tx);

                        Assert.Equal(2, counts.ReferenceTableCount);
                        Assert.Equal(1, counts.CollectionTableCount);
                    }

                    using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                    {
                        var company = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<Company>(LegacyCompanyId);
                        company.Value.Name += " LTD";
                        await session.SaveChangesAsync();
                    }

                    Indexes.WaitForIndexing(store);

                    using (var session = store.OpenAsyncSession())
                    {
                        var results = await session.Query<CompareExchangeIndex.Result, CompareExchangeIndex>()
                            .ProjectInto<CompareExchangeIndex.Result>().ToListAsync();
                        
                        Assert.Equal(2, results.Count);
                        
                        foreach (var company in results)
                        {
                            Assert.Equal("RavenDB LTD", company.CompanyName);
                        }
                    }

                    using (var session = store.OpenAsyncSession())
                    {
                        session.Delete(LegacyEmployeeId1);
                        session.Delete(LegacyEmployeeId2);
                        await session.SaveChangesAsync();
                        // deleting the documents won't change the internal references tree like in new indexes
                    }

                    Indexes.WaitForIndexing(store);

                    using (indexInstance._contextPool.AllocateOperationContext(out TransactionOperationContext context))
                    using (var tx = context.OpenReadTransaction())
                    {
                        var counts = indexInstance._indexStorage.ReferencesForCompareExchange.GetReferenceTablesCount("Employees", tx);

                        Assert.Equal(2, counts.ReferenceTableCount);
                        Assert.Equal(1, counts.CollectionTableCount);
                    }

                    using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                    {
                        var company = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<Company>(LegacyCompanyId);
                        company.Value.Name += " HR";
                        await session.SaveChangesAsync();
                        // when we update the references, this will clean the leftovers
                    }

                    Indexes.WaitForIndexing(store);

                    using (indexInstance._contextPool.AllocateOperationContext(out TransactionOperationContext context))
                    using (var tx = context.OpenReadTransaction())
                    {
                        var counts = indexInstance._indexStorage.ReferencesForCompareExchange.GetReferenceTablesCount("Employees", tx);

                        Assert.Equal(0, counts.ReferenceTableCount);
                        Assert.Equal(0, counts.CollectionTableCount);
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Indexes, Skip = "https://issues.hibernatingrhinos.com/issue/RavenDB-23166/Snapshot-restore-of-with-compare-exchange-references")]
        public async Task Can_Delete_CompareExchange_References_For_Legacy_Index_Map_Reduce()
        {
            var backupPath = NewDataPath(forceCreateDir: true);
            var fullBackupPath = Path.Combine(backupPath, "2024-11-19-10-39-25-6242058.ravendb-snapshot");

            await using (var file = File.Create(fullBackupPath))
            {
                await using (var stream = typeof(RavenDB_23100).Assembly.GetManifestResourceStream("SlowTests.Data.RavenDB_23100.2024-11-19-10-39-25-6242058.ravendb-snapshot"))
                {
                    await stream.CopyToAsync(file);
                }
            }

            using (var store = GetDocumentStore(new Options
            {
                CreateDatabase = false
            }))
            {
                using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration
                {
                    BackupLocation = backupPath,
                    DatabaseName = store.Database
                }))
                {
                    var database = await GetDatabase(store.Database);
                    var indexInstance = database.IndexStore.GetIndex(new CompareExchangeMapReduceIndex().IndexName);

                    using (indexInstance._contextPool.AllocateOperationContext(out TransactionOperationContext context))
                    using (var tx = context.OpenReadTransaction())
                    {
                        var counts = indexInstance._indexStorage.ReferencesForCompareExchange.GetReferenceTablesCount("Employees", tx);

                        Assert.Equal(2, counts.ReferenceTableCount);
                        Assert.Equal(1, counts.CollectionTableCount);
                    }

                    using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                    {
                        var company = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<Company>(LegacyCompanyId);
                        company.Value.Name += " LTD";
                        await session.SaveChangesAsync();
                    }

                    Indexes.WaitForIndexing(store);

                    using (var session = store.OpenAsyncSession())
                    {
                        var results = await session.Query<CompareExchangeMapReduceIndex.Result, CompareExchangeMapReduceIndex>().ToListAsync();
                        Assert.Equal(1, results.Count);
                        Assert.Equal("RavenDB LTD", results[0].CompanyName);
                    }

                    using (var session = store.OpenAsyncSession())
                    {
                        session.Delete(LegacyEmployeeId1);
                        session.Delete(LegacyEmployeeId2);
                        await session.SaveChangesAsync();
                        // deleting the documents won't change the internal references tree like in new indexes
                    }

                    Indexes.WaitForIndexing(store);

                    using (indexInstance._contextPool.AllocateOperationContext(out TransactionOperationContext context))
                    using (var tx = context.OpenReadTransaction())
                    {
                        var counts = indexInstance._indexStorage.ReferencesForCompareExchange.GetReferenceTablesCount("Employees", tx);

                        Assert.Equal(2, counts.ReferenceTableCount);
                        Assert.Equal(1, counts.CollectionTableCount);
                    }

                    using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                    {
                        var company = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<Company>(LegacyCompanyId);
                        company.Value.Name += " HR";
                        await session.SaveChangesAsync();
                        // when we update the references, this will clean the leftovers
                    }

                    Indexes.WaitForIndexing(store);

                    using (indexInstance._contextPool.AllocateOperationContext(out TransactionOperationContext context))
                    using (var tx = context.OpenReadTransaction())
                    {
                        var counts = indexInstance._indexStorage.ReferencesForCompareExchange.GetReferenceTablesCount("Employees", tx);

                        Assert.Equal(0, counts.ReferenceTableCount);
                        Assert.Equal(0, counts.CollectionTableCount);
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public async Task Can_Delete_Counters_References()
        {
            using (var store = GetDocumentStore())
            {
                var index = new CountersIndex();

                await store.ExecuteIndexAsync(index);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(_company);

                    await session.StoreAsync(_employee1);
                    session.CountersFor(_employee1.Id).Increment(CompanyId);

                    await session.StoreAsync(_employee2);
                    session.CountersFor(_employee2.Id).Increment(CompanyId);

                    await session.SaveChangesAsync();
                }

                Indexes.WaitForIndexing(store);

                var database = await GetDatabase(store.Database);
                var indexInstance = database.IndexStore.GetIndex(index.IndexName);

                using (indexInstance._contextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (var tx = context.OpenReadTransaction())
                {
                    var counts = indexInstance._indexStorage.ReferencesForDocuments.GetReferenceTablesCount("Employees", tx);

                    Assert.Equal(2, counts.ReferenceTableCount);
                    Assert.Equal(1, counts.CollectionTableCount);
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.Delete(_employee1.Id);
                    await session.SaveChangesAsync();
                }

                Indexes.WaitForIndexing(store);

                using (indexInstance._contextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (var tx = context.OpenReadTransaction())
                {
                    var counts = indexInstance._indexStorage.ReferencesForDocuments.GetReferenceTablesCount("Employees", tx);

                    Assert.Equal(1, counts.ReferenceTableCount);
                    Assert.Equal(1, counts.CollectionTableCount);
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.Delete(_employee2.Id);
                    await session.SaveChangesAsync();
                }

                Indexes.WaitForIndexing(store);

                using (indexInstance._contextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (var tx = context.OpenReadTransaction())
                {
                    var counts = indexInstance._indexStorage.ReferencesForDocuments.GetReferenceTablesCount("Employees", tx);

                    Assert.Equal(0, counts.ReferenceTableCount);
                    Assert.Equal(0, counts.CollectionTableCount);
                }

            }
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public async Task Can_Delete_TimeSeries_References()
        {
            using (var store = GetDocumentStore())
            {
                var index = new TimeSeriesIndex();

                await store.ExecuteIndexAsync(index);

                using (var session = store.OpenAsyncSession())
                {
                    var baseDate = DateTime.UtcNow;

                    await session.StoreAsync(_employee1);
                    session.TimeSeriesFor(_employee1.Id, CompanyId).Append(baseDate, 1, CompanyId);

                    await session.StoreAsync(_employee2);
                    session.TimeSeriesFor(_employee2.Id, CompanyId).Append(baseDate, 1, CompanyId);

                    await session.SaveChangesAsync();
                }

                Indexes.WaitForIndexing(store);

                var database = await GetDatabase(store.Database);
                var indexInstance = database.IndexStore.GetIndex(index.IndexName);

                using (indexInstance._contextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (var tx = context.OpenReadTransaction())
                {
                    var counts = indexInstance._indexStorage.ReferencesForDocuments.GetReferenceTablesCount("Companies", tx);

                    Assert.Equal(2, counts.ReferenceTableCount);
                    Assert.Equal(0, counts.CollectionTableCount);

                    counts = indexInstance._indexStorage.ReferencesForDocuments.GetReferenceTablesCount("Employees", tx);

                    Assert.Equal(2, counts.ReferenceTableCount);
                    Assert.Equal(1, counts.CollectionTableCount);
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.Delete(_employee1.Id);
                    await session.SaveChangesAsync();
                }

                Indexes.WaitForIndexing(store);

                using (indexInstance._contextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (var tx = context.OpenReadTransaction())
                {
                    var counts = indexInstance._indexStorage.ReferencesForDocuments.GetReferenceTablesCount("Companies", tx);

                    Assert.Equal(1, counts.ReferenceTableCount);
                    Assert.Equal(0, counts.CollectionTableCount);

                    counts = indexInstance._indexStorage.ReferencesForDocuments.GetReferenceTablesCount("Employees", tx);

                    Assert.Equal(1, counts.ReferenceTableCount);
                    Assert.Equal(1, counts.CollectionTableCount);
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.Delete(_employee2.Id);
                    await session.SaveChangesAsync();
                }

                Indexes.WaitForIndexing(store);

                using (indexInstance._contextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (var tx = context.OpenReadTransaction())
                {
                    var counts = indexInstance._indexStorage.ReferencesForDocuments.GetReferenceTablesCount("Companies", tx);

                    Assert.Equal(0, counts.ReferenceTableCount);
                    Assert.Equal(0, counts.CollectionTableCount);

                    counts = indexInstance._indexStorage.ReferencesForDocuments.GetReferenceTablesCount("Employees", tx);

                    Assert.Equal(0, counts.ReferenceTableCount);
                    Assert.Equal(0, counts.CollectionTableCount);
                }

            }
        }

        private class Company
        {
            public string Id { get; set; }

            public string Name { get; set; }
        }

        private class Employee
        {
            public string Id { get; set; }

            public string CompanyId { get; set; }
        }

        private class DocumentsIndex : AbstractIndexCreationTask<Employee, DocumentsIndex.Result>
        {
            public class Result
            {
                public string CompanyName { get; set; }
            }

            public DocumentsIndex()
            {
                Map = employees => from employee in employees
                    select new Result
                    {
                        CompanyName = LoadDocument<Company>(employee.CompanyId).Name,
                    };

                StoreAllFields(FieldStorage.Yes);
            }
        }

        private class DocumentsIndexMapReduce : AbstractIndexCreationTask<Employee, DocumentsIndexMapReduce.Result>
        {
            public class Result
            {
                public string CompanyName { get; set; }
                public int Count { get; set; }
            }

            public DocumentsIndexMapReduce()
            {
                Map = employees => from employee in employees
                    select new Result
                    {
                        CompanyName = LoadDocument<Company>(employee.CompanyId).Name,
                        Count = 1
                    };

                Reduce = results => from result in results
                    group result by result.CompanyName into g
                    select new Result
                    {
                        CompanyName = g.Key,
                        Count = g.Sum(x => x.Count)
                    };
            }
        }

        private class CompareExchangeIndex : AbstractIndexCreationTask<Employee, CompareExchangeIndex.Result>
        {
            public class Result
            {
                public string CompanyName { get; set; }
            }

            public CompareExchangeIndex()
            {
                Map = employees =>
                    from employee in employees
                    select new Result
                    {
                        CompanyName = LoadCompareExchangeValue<Company>(employee.CompanyId).Name
                    };

                StoreAllFields(FieldStorage.Yes);
            }
        }

        private class CompareExchangeMapReduceIndex : AbstractIndexCreationTask<Employee, CompareExchangeMapReduceIndex.Result>
        {
            public class Result
            {
                public string CompanyName { get; set; }

                public int Count { get; set; }
            }

            public CompareExchangeMapReduceIndex()
            {
                Map = employees =>
                    from employee in employees
                    select new Result
                    {
                        CompanyName = LoadCompareExchangeValue<Company>(employee.CompanyId).Name,
                        Count = 1
                    };

                Reduce = results => from result in results
                    group result by result.CompanyName into g
                    select new Result
                    {
                        CompanyName = g.Key,
                        Count = g.Sum(x => x.Count)
                    };

                StoreAllFields(FieldStorage.Yes);
            }
        }

        private class CountersIndex : AbstractCountersIndexCreationTask<Employee>
        {
            public class Result
            {
                public string CompanyName { get; set; }
            }

            public CountersIndex()
            {
                AddMap(CompanyId,
                    counters => from counter in counters
                        select new Result
                        {
                            CompanyName = LoadDocument<Company>(counter.Name).Name
                        });
            }
        }

        private class TimeSeriesIndex : AbstractTimeSeriesIndexCreationTask<Employee>
        {
            public class Result
            {
                public string CompanyName { get; set; }
            }

            public TimeSeriesIndex()
            {
                AddMap(
                    CompanyId,
                    timeSeries => from ts in timeSeries
                        from entry in ts.Entries
                        select new Result
                        {
                            CompanyName = LoadDocument<Company>(entry.Tag).Name
                        });
            }
        }
    }

    
}
