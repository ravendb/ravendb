using System.Threading.Tasks;
using Xunit;

namespace FastTests.Server
{
    public class DatabaseLandlordBugs : RavenLowLevelTestBase
    {
        [Fact]
        public async Task CanCreateAndDeleteDbWithNameBeingPrefix()
        {
            var databaseWithName_2 = CreateDatabaseWithName(true, null, null, "CanCreateAndDeleteDbWithNameBeingPrefix.ctor_2");
            Assert.NotNull(databaseWithName_2);
            var databaseWithName_21 = CreateDatabaseWithName(true, null, null, "CanCreateAndDeleteDbWithNameBeingPrefix.ctor_21");
            Assert.NotNull(databaseWithName_21);

            DeleteDatabase("CanCreateAndDeleteDbWithNameBeingPrefix.ctor_2");
            Server.ServerStore.DatabasesLandlord.UnloadDirectly("CanCreateAndDeleteDbWithNameBeingPrefix.ctor_21");
            var database = await GetDatabase("CanCreateAndDeleteDbWithNameBeingPrefix.ctor_21");

            Assert.NotNull(database);
        }

    }
}
