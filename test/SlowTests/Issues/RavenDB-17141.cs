using System.Threading.Tasks;
using FastTests;
using Raven.Client.ServerWide.Operations;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17141 : RavenTestBase
    {
        public RavenDB_17141(ITestOutputHelper output) : base(output)
        {
        }
        
        [Fact]
        public async Task EnsureDatabasesCacheNotLeaking()
        {
            UseNewLocalServer();
            using (var store = GetDocumentStore(new Options{RunInMemory = false}))
            {
                for (int i = 0; i < 100; i++)
                {
                    var result = await store.Maintenance.Server.SendAsync(new ToggleDatabasesStateOperation(store.Database, disable: true));
                    Assert.True(result.Success);
                    Assert.True(result.Disabled);
                    
                    result = await store.Maintenance.Server.SendAsync(new ToggleDatabasesStateOperation(store.Database, disable: false));
                    Assert.True(result.Success);
                    Assert.False(result.Disabled);
                }

                Assert.Equal(1, Server.ServerStore.DatabasesLandlord.DatabasesCache.DetailsCount);
            }
        }

        [Fact]
        public async Task CanDeleteDisabledDatabase()
        {
            using (var store = GetDocumentStore(new Options
            {
                RunInMemory = false, 
                DeleteDatabaseOnDispose = false
            }))
            {
                var result = await store.Maintenance.Server.SendAsync(new ToggleDatabasesStateOperation(store.Database, disable: true));
                Assert.True(result.Success);
                Assert.True(result.Disabled);

                await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(store.Database, hardDelete: true));

                Assert.False(Server.ServerStore.Cluster.DatabaseExists(store.Database));
            }
        }
    }
}
