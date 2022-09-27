using System.Threading.Tasks;
using Npgsql;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Integrations.PostgreSQL;

public class RavenDB_19256 : PostgreSqlIntegrationTestBase
{
    public RavenDB_19256(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public async Task Cannot_Connect_To_Sharded_Database()
    {
        const string query = "from Employees";

        DoNotReuseServer(EnablePostgresSqlSettings);

        using (var store = Sharding.GetDocumentStore())
        {
            var e = await Assert.ThrowsAsync<PostgresException>(() => Act(store, query, Server));

            Assert.Contains("is a sharded database and does not support PostgreSQL", e.Detail);
        }
    }
}
