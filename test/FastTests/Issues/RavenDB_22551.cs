using System.Threading.Tasks;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues
{
    public class RavenDB_22551 : RavenTestBase
    {
        public RavenDB_22551(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Core)]
        public async Task Different_Casing_Should_Not_Create_New_Database_Instance()
        {
            using (var store = GetDocumentStore())
            {
                var db1 = await GetDatabase(store.Database);
                var db2 = await GetDatabase(store.Database.ToLower());

                Assert.Same(db1, db2);

                Assert.True(Server.ServerStore.DatabasesLandlord.DatabasesCache.TryGetValue(store.Database, out _));
                Assert.True(Server.ServerStore.DatabasesLandlord.DatabasesCache.TryGetValue(store.Database.ToLower(), out _));
            }
        }
    }
}
