using System;
using System.Threading.Tasks;
using BenchmarkTests.Utils;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
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
        public async Task Simple_Patch_1M(string dbNamePostfix = "", int count = 1_000_000)
        {
            using (var store = GetSimpleDocumentStore($"1M_Companies_Patch{dbNamePostfix}"))
            {
                var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
                Assert.Equal(count+1, stats.CountOfDocuments); // + hilo
                
                var operation = await store
                    .Operations
                    .SendAsync(new PatchByQueryOperation(
                        @"from Companies 
update 
{ 
    /*let count = Math.pow(10, 7);
    let l = [];
    for (let i=0; i<count; i++) {
        l.push(Math.pow(i, 2));
    }
    let res = l.reduce((x, a) => x+a, 0);*/

    this.Name = this.Name + '_patched'; // + res;
    
    this.Contacts = this.Contacts || [];
    this.Contacts.push({
        Email : 'email@email.com',
        FirstName : 'FirstName',
        Surname : 'Surname'
    });
}"));

                await operation.WaitForCompletionAsync<BulkOperationResult>(DefaultTestOperationTimeout);

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1-A");
                    

                    //Assert.True(company.Name.EndsWith("_patched"));
                    Assert.Equal(1, company.Contacts.Count);
                    Assert.Equal("email@email.com", company.Contacts[0].Email);
                    Assert.Equal("FirstName", company.Contacts[0].FirstName);
                    Assert.Equal("Surname", company.Contacts[0].Surname);
                }
            }
        }

        [Fact]
        public async Task Simple_Patch_Put_1M(string dbNamePostfix = "", int count = 1_000_000)
        {
            using (var store = GetSimpleDocumentStore($"1M_Companies_Patch_Put{dbNamePostfix}"))
            {
                var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
                Assert.Equal(count + 1, stats.CountOfDocuments); // + hilo

                var operation = await store
                    .Operations
                    .SendAsync(new PatchByQueryOperation(
                        @"from Companies 
update 
{ 
    put('companies/', this);
}"));

                await operation.WaitForCompletionAsync<BulkOperationResult>(DefaultTestOperationTimeout);

                stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
                Assert.Equal(2*count + 1, stats.CountOfDocuments); // + hilo
            }
        }

        [Fact]
        public async Task Simple_Patch_Delete_1M(string dbNamePostfix = "", int count = 1_000_000)
        {
            using (var store = GetSimpleDocumentStore($"1M_Companies_Patch_Delete{dbNamePostfix}"))
            {
                var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
                Assert.Equal(count + 1, stats.CountOfDocuments); // + hilo

                var operation = await store
                    .Operations
                    .SendAsync(new PatchByQueryOperation(
                        @"from Companies c
update 
{ 
    del(id(c));
}"));

                await operation.WaitForCompletionAsync<BulkOperationResult>(DefaultTestOperationTimeout);

                stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
                Assert.Equal(1, stats.CountOfDocuments); // + hilo
            }
        }

        public override async Task InitAsync(DocumentStore store, string dbNamePostfix = "", Options options = null, int count = 1_000_000)
        {

            try
            {
                var doc1 = CreateDatabaseRecord($"1M_Companies_Patch{dbNamePostfix}");
                options?.ModifyDatabaseRecord?.Invoke(doc1);
                await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(doc1));

                using (var bulkInsert1 = store.BulkInsert($"1M_Companies_Patch{dbNamePostfix}"))
                {
                    for (int i = 0; i < count; i++)
                    {
                        await bulkInsert1.StoreAsync(EntityFactory.CreateCompanySmall(i));
                    }
                }

            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }

            try
            {
                var doc2 = CreateDatabaseRecord($"1M_Companies_Patch_Put{dbNamePostfix}");
                options?.ModifyDatabaseRecord?.Invoke(doc2);
                await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(doc2));

                using (var bulkInsert2 = store.BulkInsert($"1M_Companies_Patch_Put{dbNamePostfix}"))
                {
                    for (int i = 0; i < count; i++)
                    {
                        await bulkInsert2.StoreAsync(EntityFactory.CreateCompanySmall(i));
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }

            try
            {
                var doc3 = CreateDatabaseRecord($"1M_Companies_Patch_Delete{dbNamePostfix}");
                options?.ModifyDatabaseRecord?.Invoke(doc3);
                await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(doc3));
            
                using (var bulkInsert3 = store.BulkInsert($"1M_Companies_Patch_Delete{dbNamePostfix}"))
                {
                    for (int i = 0; i < count; i++)
                    {
                        await bulkInsert3.StoreAsync(EntityFactory.CreateCompanySmall(i));
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }
    }
}
