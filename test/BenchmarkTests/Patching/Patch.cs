using System;
using System.Threading.Tasks;
using BenchmarkTests.Utils;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace BenchmarkTests.Patching
{
    public class Patch : BenchmarkTestBase
    {
        public Patch(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Simple_Patch_1M()
        {
            using (var store = GetSimpleDocumentStore("1M_Companies_Patch"))
            {
                var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
                Assert.Equal(1_000_001, stats.CountOfDocuments); // + hilo

                var operation = await store
                    .Operations
                    .SendAsync(new PatchByQueryOperation(
                        @"from Companies 
update 
{ 
    this.Name = this.Name + '_patched';
    
    this.Contacts = this.Contacts || [];
    this.Contacts.push({
        Email : 'email@email.com',
        FirstName : 'FirstName',
        Surname : 'Surname'
    });
}"));

                await operation.WaitForCompletionAsync<BulkOperationResult>(TimeSpan.FromMinutes(10));

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1-A");
                    

                    Assert.True(company.Name.EndsWith("_patched"));
                    Assert.Equal(1, company.Contacts.Count);
                    Assert.Equal("email@email.com", company.Contacts[0].Email);
                    Assert.Equal("FirstName", company.Contacts[0].FirstName);
                    Assert.Equal("Surname", company.Contacts[0].Surname);
                }
            }
        }

        [Fact]
        public async Task Simple_Patch_Put_1M()
        {
            using (var store = GetSimpleDocumentStore("1M_Companies_Patch_Put"))
            {
                var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
                Assert.Equal(1_000_001, stats.CountOfDocuments); // + hilo

                var operation = await store
                    .Operations
                    .SendAsync(new PatchByQueryOperation(
                        @"from Companies 
update 
{ 
    put('companies/', this);
}"));

                await operation.WaitForCompletionAsync<BulkOperationResult>(TimeSpan.FromMinutes(10));

                stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
                Assert.Equal(2_000_001, stats.CountOfDocuments); // + hilo
            }
        }

        [Fact]
        public async Task Simple_Patch_Delete_1M()
        {
            using (var store = GetSimpleDocumentStore("1M_Companies_Patch_Delete"))
            {
                var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
                Assert.Equal(1_000_001, stats.CountOfDocuments); // + hilo

                var operation = await store
                    .Operations
                    .SendAsync(new PatchByQueryOperation(
                        @"from Companies c
update 
{ 
    del(id(c));
}"));

                await operation.WaitForCompletionAsync<BulkOperationResult>(TimeSpan.FromMinutes(10));

                stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
                Assert.Equal(1, stats.CountOfDocuments); // + hilo
            }
        }

        public override async Task InitAsync(DocumentStore store)
        {
            await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord("1M_Companies_Patch")));
            await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord("1M_Companies_Patch_Put")));
            await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord("1M_Companies_Patch_Delete")));

            using (var bulkInsert1 = store.BulkInsert("1M_Companies_Patch"))
            using (var bulkInsert2 = store.BulkInsert("1M_Companies_Patch_Put"))
            using (var bulkInsert3 = store.BulkInsert("1M_Companies_Patch_Delete"))
            {
                for (int i = 0; i < 1_000_000; i++)
                {
                    await bulkInsert1.StoreAsync(EntityFactory.CreateCompanySmall(i));

                    await bulkInsert2.StoreAsync(EntityFactory.CreateCompanySmall(i));

                    await bulkInsert3.StoreAsync(EntityFactory.CreateCompanySmall(i));
                }
            }
        }
    }
}
