#if RUN_NPGSQL_TESTS
using System.Threading.Tasks;
using Npgsql;
using Xunit;

namespace EmbeddedTests.Server.Integrations.PostgreSQL
{
    public class RavenDB_17478 : PostgreSqlIntegrationTestBase
    {
        [Fact]
        public async Task ShouldRejectDeclareFunctionWithSemicolonsInBody_WithJsFragmentDiagnostic()
        {
            const string query =
                @"declare function name(e) {
                    if (!e)
                        return null;
                    return e.FirstName + "" "" + e.LastName;
                }
                from Employees as e
                where id() in ('employees/2-A', 'employees/1-A'  )
                load e.ReportsTo as boss
                select { Name: name(e), Manager: name(boss) }";

            using (var store = GetDocumentStore())
            {
                var pgException = await Assert.ThrowsAsync<PostgresException>(async () => await Act(store, query));

                Assert.Equal("0A000", pgException.SqlState);
                Assert.Contains("declare function", pgException.MessageText);
                Assert.Contains("Remove the semicolons from the JS body", pgException.MessageText);
            }
        }
    }
}
#endif
