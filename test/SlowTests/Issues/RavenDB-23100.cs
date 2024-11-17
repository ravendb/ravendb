using System;
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

        [RavenFact(RavenTestCategory.Indexes)]
        public async Task Can_Delete_Document_References()
        {
            using (var store = GetDocumentStore())
            {
                var index = new DocumentsIndex();
                await store.ExecuteIndexAsync(index);

                var company = new Company { Id = "COMan!es/1-A", Name = "RavenDB" };

                var employee1 = new Employee { Id = "Employ&es/1-A", CompanyId = company.Id };
                var employee2 = new Employee { Id = "EmPlo#ees/2-A", CompanyId = company.Id };

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(company);
                    await session.StoreAsync(employee1);
                    await session.StoreAsync(employee2);
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
