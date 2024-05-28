#if NET8_0
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace EmbeddedTests.Server.Integrations.PostgreSQL
{
    public class RavenDB_19470 : PostgreSqlIntegrationTestBase
    {
        private List<string> GetColumnNames(DataTable dataTable)
        {
            return dataTable.Columns
                .Cast<DataColumn>()
                .Select(x => x.ColumnName)
                .ToList();
        }
        
        [Fact]
        public async Task ForSpecificDatabase_AndSpecificQuery_GetCorrectSelectedFields_NamedStatements()
        {
            const string firstField = "FirstName";
            const string secondField = "LastName";
            string query = $"from Employees select {firstField}, {secondField}";

            using (var store = GetDocumentStore())
            {
                await store.Maintenance.SendAsync(new CreateSampleDataOperation());

                var result = await Act(store, query, prepareExecute: true);

                Assert.NotNull(result);
                Assert.NotEmpty(result.Columns);

                var columns = GetColumnNames(result);
                Assert.Contains(firstField, columns);
                Assert.Contains(secondField, columns);
            }
        }
    }
}
#endif
