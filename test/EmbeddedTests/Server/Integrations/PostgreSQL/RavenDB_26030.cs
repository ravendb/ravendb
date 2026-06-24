#if RUN_NPGSQL_TESTS
using System.Threading.Tasks;
using Xunit;

namespace EmbeddedTests.Server.Integrations.PostgreSQL;

public class RavenDB_26030 : PostgreSqlIntegrationTestBase
{
    [Fact]
    public async Task Simple_select_where_should_be_translated_via_ast_and_return_rows()
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "oren" }, "users/1-A", TestContext.Current.CancellationToken);
                await session.StoreAsync(new User { Name = "ayende" }, "users/2-A", TestContext.Current.CancellationToken);
                await session.SaveChangesAsync(TestContext.Current.CancellationToken);
            }

            // Identifiers must be quoted to preserve case for RavenDB's case-sensitive field lookup.
            // Unquoted identifiers follow PostgreSQL semantics (folded to lowercase by pgsqlparser).
            const string sql = "SELECT * FROM \"Users\" WHERE \"Name\" = 'oren'";

            var result = await Act(store, sql);

            Assert.NotNull(result);
            Assert.NotEmpty(result.Rows);
            Assert.Equal(1, result.Rows.Count);
        }
    }

    private sealed class User
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }
}
#endif
