#if RUN_NPGSQL_TESTS
using System.Threading.Tasks;
using Npgsql;
using Xunit;

namespace EmbeddedTests.Server.Integrations.PostgreSQL
{
    // Apache Superset connects through SQLAlchemy + psycopg2, which runs an hstore probe on
    // EVERY connect to decide whether to enable native hstore. In real PostgreSQL it returns 0
    // rows (there is no hstore type) and the client proceeds. RavenDB used to reject it — the
    // JOIN over pg_type/pg_namespace bailed because pg_type lacked the projected `typarray`
    // column, and the query got mislabeled as an unsupported "JOIN over RavenDB collections",
    // so Superset never finished connecting (Zoho Desk #7031).
    //
    // These exercise the full wire path (real NpgsqlConnection -> PgSession -> PgQuery), unlike
    // the in-process PgVirtualInterpreterTests unit coverage.
    public class SqlAlchemyConnectivityTests : PostgreSqlIntegrationTestBase
    {
        // The exact query SQLAlchemy's psycopg2 dialect sends.
        private const string HstoreProbe =
            "SELECT t.oid, typarray FROM pg_type t JOIN pg_namespace ns ON typnamespace = ns.oid WHERE typname = 'hstore'";

        [Fact]
        public async Task Hstore_probe_returns_empty_two_column_rowset_without_error()
        {
            using var store = GetDocumentStore();

            var result = await Act(store, HstoreProbe);

            Assert.NotNull(result);
            Assert.Equal(2, result.Columns.Count);
            Assert.Equal(0, result.Rows.Count);
        }

        [Fact]
        public async Task Connection_stays_usable_for_data_queries_after_hstore_probe()
        {
            const int documentCount = 5;

            using var store = GetDocumentStore();

            using (var session = store.OpenSession())
            {
                for (int i = 0; i < documentCount; i++)
                    session.Store(new Employee { LastName = $"Emp_{i}" }, $"employees/{i}");
                session.SaveChanges();
            }

            var connectionString = GetConnectionString(store);
            await using var connection = new NpgsqlConnection(connectionString);
#pragma warning disable xUnit1051 // Calls to methods which accept CancellationToken should use TestContext.Current.CancellationToken
            await connection.OpenAsync();

            // 1) The hstore probe, on the same connection SQLAlchemy would keep open.
            await using (var probe = new NpgsqlCommand(HstoreProbe, connection))
            await using (var probeReader = await probe.ExecuteReaderAsync())
            {
                Assert.False(await probeReader.ReadAsync(), "hstore probe must return no rows");
            }

            // 2) A real RavenDB query over the SAME connection must still work afterwards.
            await using (var cmd = new NpgsqlCommand("from Employees", connection))
            await using (var reader = await cmd.ExecuteReaderAsync())
            {
                int rows = 0;
                while (await reader.ReadAsync())
                    rows++;
                Assert.Equal(documentCount, rows);
            }
#pragma warning restore xUnit1051
        }
    }
}
#endif
