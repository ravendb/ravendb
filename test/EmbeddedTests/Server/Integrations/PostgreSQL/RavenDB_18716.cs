#if RUN_NPGSQL_TESTS
using System.Threading.Tasks;
using Xunit;
#pragma warning disable xUnit1051

namespace EmbeddedTests.Server.Integrations.PostgreSQL;

public class RavenDB_18716 : PostgreSqlIntegrationTestBase
{
    
    [Fact]
    public async Task CanResponseToSelectVersionAndSettingsQuery()
    {
        const string postgresQuery = "select version();select current_setting('max_index_keys');";

        using (var store = GetDocumentStore())
        {
            await store.Maintenance.SendAsync(new CreateSampleDataOperation());

            var result = await Act(store, postgresQuery);
            Assert.Equal(1, result.Rows.Count);
            Assert.Equal(1, result.Columns.Count);
        }
    }
}
#endif
