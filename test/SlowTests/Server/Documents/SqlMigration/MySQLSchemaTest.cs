using System.Linq;
using Raven.Server.SqlMigration;
using Raven.Server.SqlMigration.Schema;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.SqlMigration
{
    public class MySQLSchemaTest : SqlAwareTestBase
    {
        [RequiresMySqlFact]
        public void CanFetchSchema()
        {
            using (WithSqlDatabase(MigrationProvider.MySQL, out var connectionString, out string schemaName, includeData: false))
            {
                var driver = DatabaseDriverDispatcher.CreateDriver(MigrationProvider.MySQL, connectionString);
                var schema = driver.FindSchema();
                Assert.NotNull(schema.CatalogName);

                Assert.Equal(21, schema.Tables.Count);

                // validate NoPkTable

                var tables = schema.Tables;

                var noPkTable = tables.First(x => x.TableName == "nopktable");
                Assert.NotNull(noPkTable);

                Assert.Equal(new[] {"id"}, noPkTable.Columns.Select(x => x.Name).ToList());
                Assert.Equal(0, noPkTable.PrimaryKeyColumns.Count);

                // validate Order Table
                var orderTable = tables.First(x => x.TableName == "orders");

                Assert.Equal(20, orderTable.Columns.Count);
                Assert.Equal(new[] {"id"}, orderTable.PrimaryKeyColumns);

                var orderReferences = orderTable.References;
                Assert.Equal(3, orderReferences.Count);
                Assert.Equal(
                    new[]
                    {
                        "inventory_transactions -> (customer_order_id)",
                        "invoices -> (order_id)",
                        "order_details -> (order_id)"
                    },
                    orderReferences.Select(x => x.Table + " -> (" + string.Join(",", x.Columns) + ")").ToList());

                // validate employee_privileges (2 columns in PK)
                var employeePrivileges = tables.First(x => x.TableName == "employee_privileges");
                Assert.Equal(new[] {"employee_id", "privilege_id"}, employeePrivileges.PrimaryKeyColumns);

                Assert.True(tables.All(x => x.Columns.All(y => y.Type != ColumnType.Unsupported)));
            }
        }
    }
}
