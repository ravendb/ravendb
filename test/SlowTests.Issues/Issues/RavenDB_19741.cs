using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.ServerWide.Operations.Configuration;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_19741 : RavenTestBase
{
    public RavenDB_19741(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public async Task Will_Respect_Server_Wide_Client_Configuration_For_IdentityPartsSeparator()
    {
        UseNewLocalServer();

        using (var store = GetDocumentStore())
        {
            await store.Maintenance.Server.SendAsync(new PutServerWideClientConfigurationOperation(new ClientConfiguration
            {
                Disabled = true,
                IdentityPartsSeparator = '^'
            }));

            var database = await GetDatabase(store.Database);

            Assert.Equal(Constants.Identities.DefaultSeparator, database.IdentityPartsSeparator);

            await store.Maintenance.Server.SendAsync(new PutServerWideClientConfigurationOperation(new ClientConfiguration
            {
                Disabled = false,
                IdentityPartsSeparator = '^'
            }));

            Assert.Equal('^', database.IdentityPartsSeparator);

            Server.ServerStore.DatabasesLandlord.UnloadDirectly(store.Database);

            database = await GetDatabase(store.Database);

            Assert.Equal('^', database.IdentityPartsSeparator);
        }
    }
}
