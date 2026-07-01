#if RUN_NPGSQL_TESTS
using System;
using System.Data;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Xunit;

namespace EmbeddedTests.Server.Integrations.PostgreSQL;

public class RavenDB_26286 : PostgreSqlIntegrationTestBase
{
    [Fact]
    public async Task FetchImport_SqlTextbox_InnerSqlSelectWithFilter_ReturnsMatchingRows()
    {
        using (var store = GetDocumentStore())
        {
            await store.Maintenance.SendAsync(new CreateSampleDataOperation(), TestContext.Current.CancellationToken);

            const string sql =
                "select *\n" +
                "from\n" +
                "(\n" +
                "select \"FirstName\", \"LastName\"\n" +
                "from \"Employees\"\n" +
                "where \"Title\" = 'Sales Representative'\n" +
                ") \"_\"\n" +
                "limit 100";

            var result = await Act(store, sql);

            Assert.NotNull(result);
            Assert.NotEmpty(result.Rows);

            Assert.True(result.Columns.Contains("FirstName"), "Expected 'FirstName' column");
            Assert.True(result.Columns.Contains("LastName"), "Expected 'LastName' column");

            // Northwind has 9 employees total; the filter must narrow the result.
            Assert.True(result.Rows.Count < 9,
                $"Expected fewer than 9 rows but got {result.Rows.Count}");

            foreach (DataRow row in result.Rows)
                Assert.False(string.IsNullOrWhiteSpace(row["FirstName"]?.ToString()));
        }
    }

    [Fact]
    public async Task FetchImport_SqlTextbox_InnerSqlInListFilter_ReturnsMatchingRows()
    {
        using (var store = GetDocumentStore())
        {
            await store.Maintenance.SendAsync(new CreateSampleDataOperation(), TestContext.Current.CancellationToken);

            const string sql =
                "select *\n" +
                "from\n" +
                "(\n" +
                "select \"Company\", \"Employee\"\n" +
                "from \"Orders\"\n" +
                "where \"Company\" IN ('Companies/1-A', 'Companies/2-A')\n" +
                ") \"_\"\n" +
                "limit 100";

            var result = await Act(store, sql);

            Assert.NotNull(result);
            Assert.NotEmpty(result.Rows);

            Assert.True(result.Columns.Contains("Company"), "Expected 'Company' column");
            Assert.True(result.Columns.Contains("Employee"), "Expected 'Employee' column");

            foreach (DataRow row in result.Rows)
            {
                var company = row["Company"]?.ToString();
                Assert.True(
                    string.Equals(company, "Companies/1-A", StringComparison.OrdinalIgnoreCase) ||
                    string.Equals(company, "Companies/2-A", StringComparison.OrdinalIgnoreCase),
                    $"Expected Company to be one of the filtered values but got '{company}'");
            }
        }
    }
}

#endif
