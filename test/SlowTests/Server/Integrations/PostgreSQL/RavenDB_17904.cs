using System;
using System.Threading.Tasks;
using Orders;
using Raven.Client.Documents;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Integrations.PostgreSQL;

public class RavenDB_17904 : PostgreSqlIntegrationTestBase
{
    public RavenDB_17904(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public async Task CanCovertDateTimeOffsetCorrectlyInPostgres()
    {
        const string query = "from Calculations";

        DoNotReuseServer(EnablePostgresSqlSettings);

        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Calculation
                {
                    CreatedAt = DateTimeOffset.Now
                });

                session.SaveChanges();
            }

            using (var session = store.OpenAsyncSession())
            {
                var employees = await session
                    .Query<Calculation>()
                    .ToListAsync();

                var result = await Act(store, query, Server);

                Assert.NotNull(result);
                Assert.NotEmpty(result.Rows);
                Assert.Equal(employees.Count, result.Rows.Count);
            }
        }
    }

    private class Calculation
    {
        public DateTimeOffset CreatedAt { get; set; }
    }
}
