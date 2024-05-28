#if NET8_0
using System;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Xunit;

namespace EmbeddedTests.Server.Integrations.PostgreSQL;

public class RavenDB_17904 : PostgreSqlIntegrationTestBase
{
    [Fact]
    public async Task CanCovertDateTimeOffsetCorrectlyInPostgres()
    {
        const string query = "from Calculations";

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

                var result = await Act(store, query);

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
#endif
