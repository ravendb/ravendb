using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Documents;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations.DocumentsCompression;
using Raven.Server.Documents.Commands.DocumentsCompression;
using Tests.Infrastructure;
using Tests.Infrastructure.Extensions;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Client.Operations
{
    public class DocumentsCompressionConfigurationTests : RavenTestBase
    {
        public DocumentsCompressionConfigurationTests(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanPostAndGetDocumentsCompressionConfiguration(Options options)
        {
            var dbname = "CompressAllCollectionsDB";
            options.ModifyDatabaseName = _ => dbname;
           
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenAsyncSession())
                {

                    var user = new User { Name = "SHR" };
                    var company = new Company { Name = "Rhino"};

                    await session.StoreAsync(user, "users/1");
                    await session.StoreAsync(company, "companies/1");

                    await session.SaveChangesAsync();
                }

                var executor = store.GetRequestExecutor();
                long originalCompanySize, originalUserSize;
                using (var _ = executor.ContextPool.AllocateOperationContext(out var ctx))
                {
                    var companySize = new DocCompression.GetDocumentSize("companies/1");
                    await executor.ExecuteAsync(companySize, ctx);
                    originalCompanySize = companySize.Result.AllocatedSize;

                    var userSize = new DocCompression.GetDocumentSize("users/1");
                    await executor.ExecuteAsync(userSize, ctx);
                    originalUserSize = userSize.Result.AllocatedSize;
                }

                //turn on compression
                store.Maintenance.Send(new UpdateDocumentsCompressionConfigurationOperation(new DocumentsCompressionConfiguration(false, true)));

                //make a change to docs so they'll be rewritten and compressed
                using (var session = store.OpenAsyncSession())
                {
                    Company comp = await session.LoadAsync<Company>("companies/1");
                    comp.Name = "Hiber";

                    User user = await session.LoadAsync<User>("users/1");
                    user.Name = "vaz";

                    await session.SaveChangesAsync();
                }

                await store.Maintenance.ForTesting(() => new GetDocumentsCompressionConfigurationOperation()).AssertAllAsync((key, documentsCompressionConfiguration) =>
                {
                    Assert.NotNull(documentsCompressionConfiguration);
                    Assert.True(documentsCompressionConfiguration.CompressAllCollections);
                    Assert.False(documentsCompressionConfiguration.CompressRevisions);
                });
            }
        }

        private class Company
        {
#pragma warning disable CS0649
            public string Id;
#pragma warning restore CS0649
            public string Name;
        }
        private class User
        {
#pragma warning disable CS0649
            public string Id;
#pragma warning restore CS0649
            public string Name;
        }
    }
}
