using System;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Client.Operations;

public class BasicChangesPatchByQueryTests : RavenTestBase
{
    public BasicChangesPatchByQueryTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.ClientApi | RavenTestCategory.Patching)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public async Task Can_Perform_Patch_By_Query_Operation(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            const int numberOfCompanies = 100;

            using (var session = store.OpenAsyncSession())
            {
                for (var i = 0; i < numberOfCompanies; i++)
                    await session.StoreAsync(new Company { Name = $"C_{i}" }, $"companies/{i}");

                await session.SaveChangesAsync();
            }

            var operation = await store.Operations.SendAsync(new PatchByQueryOperation("from Companies update { this.Name = this.Name + '_Patched'; }"));
            var result = await operation.WaitForCompletionAsync<BulkOperationResult>(TimeSpan.FromMinutes(1));

            Assert.Equal(numberOfCompanies, result.Total);

            for (var i = 0; i < numberOfCompanies; i++)
            {
                using (var session = store.OpenAsyncSession())
                {
                    var company = await session.LoadAsync<Company>($"companies/{i}");

                    Assert.NotNull(company);
                    Assert.Contains("_Patched", company.Name);
                }
            }
        }
    }
}
