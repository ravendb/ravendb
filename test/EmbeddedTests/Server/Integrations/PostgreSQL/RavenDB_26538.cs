#if RUN_NPGSQL_TESTS
using System.Threading.Tasks;
using Npgsql;
using Xunit;

namespace EmbeddedTests.Server.Integrations.PostgreSQL
{
    public class RavenDB_26538 : PostgreSqlIntegrationTestBase
    {
        [Theory]
        [InlineData(1024)]
        [InlineData(20 * 1024)]
        [InlineData(128 * 1024)]
        public async Task StreamingLargeDataset_ShouldReturnAllRows(int documentCount)
        {
            const string query = "from Items";

            using var store = GetDocumentStore();

#pragma warning disable xUnit1051 // Calls to methods which accept CancellationToken should use TestContext.Current.CancellationToken
            using (var bulkInsert = store.BulkInsert())
            {
                for (int i = 0; i < documentCount; i++)
                    bulkInsert.Store(new Item { Name = $"Item_{i}" }, $"items/{i}");
            }
#pragma warning restore xUnit1051 // Calls to methods which accept CancellationToken should use TestContext.Current.CancellationToken

            var result = await Act(store, query);

            Assert.NotNull(result);
            Assert.Equal(documentCount, result.Rows.Count);
        }

        [Fact]
        public async Task NamedStatement_ReExecuteWithoutDescribe_ShouldReturnCorrectResults()
        {
            const int documentCount = 10;
            const string query = "from Items";

            using var store = GetDocumentStore();

            using (var session = store.OpenSession())
            {
                for (int i = 0; i < documentCount; i++)
                    session.Store(new Item { Name = $"Item_{i}" }, $"items/{i}");
                session.SaveChanges();
            }

            var connectionString = GetConnectionString(store);
            await using var connection = new NpgsqlConnection(connectionString);
#pragma warning disable xUnit1051 // Calls to methods which accept CancellationToken should use TestContext.Current.CancellationToken
            await connection.OpenAsync();
#pragma warning restore xUnit1051 // Calls to methods which accept CancellationToken should use TestContext.Current.CancellationToken
            await using var cmd = new NpgsqlCommand(query, connection);

            // First execution: Parse + Describe + Bind + Execute
            cmd.Prepare();
            int firstCount = CountRows(cmd);

            // Second execution: Bind + Execute only (no Describe) — tests named statement re-execution path
            int secondCount = CountRows(cmd);

            Assert.Equal(documentCount, firstCount);
            Assert.Equal(documentCount, secondCount);
        }

        [Fact]
        public async Task QueryWithNoMatchingResults_ShouldReturnNoRows()
        {
            // Tests the streaming path when the query produces zero rows (not the _limit==0 early-exit)
            const string query = "from Employees where id() = 'employees/nonexistent'";

            using var store = GetDocumentStore();
#pragma warning disable xUnit1051 // Calls to methods which accept CancellationToken should use TestContext.Current.CancellationToken
            await store.Maintenance.SendAsync(new CreateSampleDataOperation());
#pragma warning restore xUnit1051 // Calls to methods which accept CancellationToken should use TestContext.Current.CancellationToken

            var result = await Act(store, query);

            Assert.NotNull(result);
            Assert.Equal(0, result.Rows.Count);
        }

        [Fact]
        public async Task QueryEmptyCollection_ShouldReturnNoRows()
        {
            const string query = "from Items";

            using var store = GetDocumentStore();

            var result = await Act(store, query);

            Assert.NotNull(result);
            Assert.Equal(0, result.Rows.Count);
        }

        private class Item
        {
            public string Name { get; set; }
        }

        private static int CountRows(NpgsqlCommand cmd)
        {
            int count = 0;
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
                count++;
            return count;
        }
    }
}
#endif
