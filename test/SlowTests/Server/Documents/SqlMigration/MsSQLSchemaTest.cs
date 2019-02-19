using System.Linq;
using Raven.Server.SqlMigration;
using Raven.Server.SqlMigration.Schema;
using Xunit;

namespace SlowTests.Server.Documents.SqlMigration
{
    public class MsSQLSchemaTest : SqlAwareTestBase
    {
        [Fact]
        public void CanFetchSchema()
        {
            using (WithSqlDatabase(MigrationProvider.MsSQL, out var connectionString, out string schemaName, includeData: false))
            {
                var driver = DatabaseDriverDispatcher.CreateDriver(MigrationProvider.MsSQL, connectionString);
                var schema = driver.FindSchema();

                Assert.NotNull(schema.CatalogName);
                Assert.Equal(10, schema.Tables.Count);

                // validate NoPkTable

                var tables = schema.Tables;

                var noPkTable = tables.First(x => x.TableName == "NoPkTable");
                Assert.NotNull(noPkTable);

                Assert.Equal(new[] {"Id"}, noPkTable.Columns.Select(x => x.Name).ToList());
                Assert.Equal(0, noPkTable.PrimaryKeyColumns.Count);

                // validate Order Table
                var orderTable = tables.First(x => x.TableName == "Order");

                Assert.Equal(4, orderTable.Columns.Count);
                Assert.Equal(new[] {"Id", "OrderDate", "CustomerId", "TotalAmount"}, orderTable.Columns.Select(x => x.Name).ToList());
                Assert.Equal(new[] {"Id"}, orderTable.PrimaryKeyColumns);

                var orderReferences = orderTable.References;
                Assert.Equal(1, orderReferences.Count);
                Assert.Equal("OrderItem", orderReferences[0].Table);
                Assert.Equal(new[] {"OrderId"}, orderReferences[0].Columns);

                // validate UnsupportedTable

                var unsupportedTable = tables.First(x => x.TableName == "UnsupportedTable");
                Assert.True(unsupportedTable.Columns.Any(x => x.Type == ColumnType.Unsupported));

                // validate OrderItem (2 columns in PK)

                var orderItemTable = tables.First(x => x.TableName == "OrderItem");
                Assert.Equal(new[] {"OrderId", "ProductId"}, orderItemTable.PrimaryKeyColumns);

                Assert.Equal(1, orderItemTable.References.Count);
                Assert.Equal("Details", orderItemTable.References[0].Table);
                Assert.Equal(new[] {"OrderId", "ProductId"}, orderItemTable.References[0].Columns);

                // all types are supported (except UnsupportedTable)
                Assert.True(tables.Where(x => x.TableName != "UnsupportedTable")
                    .All(x => x.Columns.All(y => y.Type != ColumnType.Unsupported)));

                // validate many - to - many
                var productsCategory = tables.First(x => x.TableName == "ProductCategory");
                Assert.Equal(0, productsCategory.References.Count);
                Assert.Equal(1, tables.First(x => x.TableName == "Category").References.Count);
                Assert.Equal(2, tables.First(x => x.TableName == "Product").References.Count);
            }
        }
    }
}
