#if NET8_0
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using Xunit;

namespace EmbeddedTests.Server.Integrations.PostgreSQL;

public class RavenDB_19256 : PostgreSqlIntegrationTestBase
{

    [Fact]
    public async Task Cannot_Connect_To_Sharded_Database()
    {
        const string query = "from Employees";

        using (var store = GetDocumentStore(sharded: true))
        {
            var e = await Assert.ThrowsAsync<PostgresException>(() => Act(store, query));

            Assert.Contains("is a sharded database and does not support PostgreSQL", e.Detail);
        }
    }
}
#endif
