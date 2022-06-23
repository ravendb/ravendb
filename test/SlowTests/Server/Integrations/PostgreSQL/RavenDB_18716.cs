using System;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Integrations.PostgreSQL;

public class RavenDB_18716 : PostgreSqlIntegrationTestBase
{
    public RavenDB_18716(ITestOutputHelper output) : base(output)
    {
    }
    
    [Fact]
    public async Task CanResponseToSelectVersionAndSettingsQuery()
    {
        const string postgresQuery = "select version();select current_setting('max_index_keys');";

        DoNotReuseServer(EnablePostgresSqlSettings);

        using (var store = GetDocumentStore())
        {
            Samples.CreateNorthwindDatabase(store);

            var result = await Act(store, postgresQuery, Server);
            Assert.Equal(1, result.Rows.Count);
            Assert.Equal(1, result.Columns.Count);
        }
    }
}
