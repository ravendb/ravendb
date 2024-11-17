using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
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

        private const string EmployeeId1 = "Employ&es/1-A";
        private const string EmployeeId2 = "EmPlo#ees/2-A";
        private const string CompanyId = "COMan!es/1-A";

        [RavenFact(RavenTestCategory.Indexes)]
        public async Task Can_Delete_Document_References()
        {
            using (var store = GetDocumentStore())
            {
                var index = new DocumentsIndex();
                await store.ExecuteIndexAsync(index);

                var company = new Company { Id = CompanyId, Name = "RavenDB" };

                var employee1 = new Employee { Id = EmployeeId1, CompanyId = company.Id };
                var employee2 = new Employee { Id = EmployeeId2, CompanyId = company.Id };

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(company);
                    await session.StoreAsync(employee1);
                    await session.StoreAsync(employee2);
                    await session.SaveChangesAsync();
                }

                Indexes.WaitForIndexing(store);

                WaitForUserToContinueTheTest(store);
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
                    session.Delete(employee1.Id);
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
                    session.Delete(employee2.Id);
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

        [RavenFact(RavenTestCategory.Indexes)]
        public async Task Can_Delete_Document_References_For_Legacy_Index()
        {
            var backupPath = NewDataPath(forceCreateDir: true);
            var fullBackupPath = Path.Combine(backupPath, "2024-11-17-09-31-49-8220303.ravendb-snapshot");

            await using (var file = File.Create(fullBackupPath))
            {
                await using (var stream = typeof(RavenDB_23100).Assembly.GetManifestResourceStream("SlowTests.Data.RavenDB_23100.2024-11-17-09-31-49-8220303.ravendb-snapshot"))
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
                        var counts = indexInstance._indexStorage.ReferencesForDocuments.GetReferenceTablesCount("Companies", tx);

                        Assert.Equal(2, counts.ReferenceTableCount);
                        Assert.Equal(0, counts.CollectionTableCount);

                        counts = indexInstance._indexStorage.ReferencesForDocuments.GetReferenceTablesCount("Employees", tx);

                        Assert.Equal(2, counts.ReferenceTableCount);
                        Assert.Equal(1, counts.CollectionTableCount);
                    }

                    using (var session = store.OpenAsyncSession())
                    {
                        session.Delete(EmployeeId1);
                        session.Delete(EmployeeId2);
                        await session.SaveChangesAsync();
                        // deleting the documents won't change the internal references tree like in new indexes
                    }

                    Indexes.WaitForIndexing(store);

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
                        var company = await session.LoadAsync<Company>(CompanyId);
                        company.Name += " LTD";
                        await session.SaveChangesAsync();
                        // when we update the references, this will clean the leftovers
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
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public async Task Can_Delete_CompareExchange_References()
        {
            using (var store = GetDocumentStore())
            {
                var index = new DocumentsWithCompareExchangeIndex();

                await store.ExecuteIndexAsync(index);

                var commonName = "Company#1";

                using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue(commonName, new Company { Name = "RavenDB" });
                    await session.SaveChangesAsync();
                }

                var employee1 = new Employee { Id = "Employ&es/1-A", CompanyId = commonName };
                var employee2 = new Employee { Id = "EmPlo#ees/2-A", CompanyId = commonName };

                using (var session = store.OpenSession())
                {
                    session.Store(employee1);
                    session.Store(employee2);
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                var database = await GetDatabase(store.Database);
                var indexInstance = database.IndexStore.GetIndex(index.IndexName);

                using (indexInstance._contextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (var tx = context.OpenReadTransaction())
                {
                    var counts = indexInstance._indexStorage.ReferencesForCompareExchange.GetReferenceTablesCount("Companies", tx);

                    Assert.Equal(2, counts.ReferenceTableCount);
                    Assert.Equal(0, counts.CollectionTableCount);

                    counts = indexInstance._indexStorage.ReferencesForCompareExchange.GetReferenceTablesCount("Employees", tx);

                    Assert.Equal(2, counts.ReferenceTableCount);
                    Assert.Equal(1, counts.CollectionTableCount);
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.Delete(employee1.Id);
                    await session.SaveChangesAsync();
                }

                Indexes.WaitForIndexing(store);

                using (indexInstance._contextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (var tx = context.OpenReadTransaction())
                {
                    var counts = indexInstance._indexStorage.ReferencesForCompareExchange.GetReferenceTablesCount("Companies", tx);

                    Assert.Equal(1, counts.ReferenceTableCount);
                    Assert.Equal(0, counts.CollectionTableCount);

                    counts = indexInstance._indexStorage.ReferencesForCompareExchange.GetReferenceTablesCount("Employees", tx);

                    Assert.Equal(1, counts.ReferenceTableCount);
                    Assert.Equal(1, counts.CollectionTableCount);
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.Delete(employee2.Id);
                    await session.SaveChangesAsync();
                }

                Indexes.WaitForIndexing(store);

                using (indexInstance._contextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (var tx = context.OpenReadTransaction())
                {
                    var counts = indexInstance._indexStorage.ReferencesForCompareExchange.GetReferenceTablesCount("Companies", tx);

                    Assert.Equal(0, counts.ReferenceTableCount);
                    Assert.Equal(0, counts.CollectionTableCount);

                    counts = indexInstance._indexStorage.ReferencesForCompareExchange.GetReferenceTablesCount("Employees", tx);

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
            }
        }

        private class DocumentsWithCompareExchangeIndex : AbstractIndexCreationTask<Employee>
        {
            public class Result
            {
                public string CompanyName { get; set; }
            }

            public DocumentsWithCompareExchangeIndex()
            {
                Map = employees =>
                    from employee in employees
                    select new Result
                    {
                        CompanyName = LoadCompareExchangeValue<Company>(employee.CompanyId).Name
                    };
            }
        }
    }

    
}
