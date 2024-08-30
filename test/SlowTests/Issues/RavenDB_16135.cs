using System;
using System.Net.Http;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Orders;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Server.Documents.Revisions;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16135 : RavenTestBase
    {
        public RavenDB_16135(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task RevertRevisions_WaitForCompletionShouldNotThrow()
        {
            var company = new Company { Name = "Company Name" };
            using (var store = GetDocumentStore())
            {
                DateTime last = default;
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(company);
                    await session.SaveChangesAsync();
                    last = DateTime.UtcNow;
                }

                using (var session = store.OpenAsyncSession())
                {
                    company.Name = "Hibernating Rhinos";
                    await session.StoreAsync(company);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var companiesRevisions = await session.Advanced.Revisions.GetForAsync<Company>(company.Id);
                    Assert.Equal(2, companiesRevisions.Count);
                }

                var operation = await store.Maintenance.SendAsync(new RevisionsHelper.RevertRevisionsOperation(last, 60));
                await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);

                using (var session = store.OpenAsyncSession())
                {
                    var companiesRevisions = await session.Advanced.Revisions.GetForAsync<Company>(company.Id);
                    Assert.Equal(3, companiesRevisions.Count);

                    Assert.Equal("Company Name", companiesRevisions[0].Name);
                    Assert.Equal("Hibernating Rhinos", companiesRevisions[1].Name);
                    Assert.Equal("Company Name", companiesRevisions[2].Name);
                }
            }
        }

    }
}
