using System.Threading.Tasks;
using FastTests;
using SlowTests.Core.AdminConsole;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17451 : RavenTestBase
    {
        public RavenDB_17451(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanModifyDisableDatabaseTcpCompressionConfiguration()
        {
            using (var store = GetDocumentStore())
            {
                var database = await GetDocumentDatabaseInstanceFor(store);
                var configuration = database.Configuration;

                Assert.False(configuration.Databases.DisableTcpCompression);

                AdminJsConsoleTests.ExecuteScript(Server, database, "database.Configuration.Databases.DisableTcpCompression = true;");

                Assert.True(configuration.Databases.DisableTcpCompression);
            }
        }

        [Fact]
        public void CanModifyDisableServerTcpCompressionConfiguration()
        {
            var configuration = Server.Configuration;

            Assert.False(configuration.Databases.DisableTcpCompression);

            AdminJsConsoleTests.ExecuteScript(Server, database: null, "server.Configuration.Databases.DisableTcpCompression = true;");

            Assert.True(configuration.Databases.DisableTcpCompression);
        }
    }
}
