using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using Raven.Server.ServerWide.Context;
using Raven.Server.SqlMigration;
using Raven.Server.SqlMigration.Model;
using Raven.Server.SqlMigration.Schema;
using Xunit;

namespace SlowTests.Server.Documents.SqlMigration
{
    public class NpgSQLSchemaTest : SqlAwareTestBase
    {
        [Fact]
        public void CanFetchSchema()
        {
            using (WithSqlDatabase(MigrationProvider.NpgSQL, out var connectionString, out string schemaName, includeData: false))
            {
                var driver = DatabaseDriverDispatcher.CreateDriver(MigrationProvider.NpgSQL, connectionString);
                var schema = driver.FindSchema();


                Assert.NotNull(schema.CatalogName);
                Assert.Equal(10, schema.Tables.Count);

                var tables = schema.Tables;

                // validate NoPkTable
                var noPkTable = tables.First(x => x.TableName == "nopktable");
                Assert.NotNull(noPkTable);

                Assert.Equal(new[] { "id" }, noPkTable.Columns.Select(x => x.Name).ToList());
                Assert.Equal(0, noPkTable.PrimaryKeyColumns.Count);

                // validate Order Table
                var orderTable = tables.First(x => x.TableName == "Order");

                Assert.Equal(4, orderTable.Columns.Count);
                Assert.Equal(new[] { "id", "orderdate", "customerid", "totalamount" }, orderTable.Columns.Select(x => x.Name).ToList());
                Assert.Equal(new[] { "id" }, orderTable.PrimaryKeyColumns);

                var orderReferences = orderTable.References;
                Assert.Equal(1, orderReferences.Count);
                Assert.Equal("orderitem", orderReferences[0].Table);
                Assert.Equal(new[] { "orderid".ToLower() }, orderReferences[0].Columns);

                // validate UnsupportedTable

                var unsupportedTable = tables.First(x => x.TableName == "unsupportedtable");
                Assert.True(unsupportedTable.Columns.Any(x => x.Type == ColumnType.Unsupported));

                // validate OrderItem (2 columns in PK)

                var orderItemTable = tables.First(x => x.TableName == "orderitem");
                Assert.Equal(new[] { "orderid", "productid" }, orderItemTable.PrimaryKeyColumns);

                Assert.Equal(1, orderItemTable.References.Count);
                Assert.Equal("details", orderItemTable.References[0].Table);
                Assert.Equal(new[] { "orderid", "productid" }, orderItemTable.References[0].Columns);

                // all types are supported (except UnsupportedTable)
                Assert.True(tables.Where(x => x.TableName != "unsupportedtable")
                    .All(x => x.Columns.All(y => y.Type != ColumnType.Unsupported)));

                // validate many - to - many
                var productsCategory = tables.First(x => x.TableName == "productcategory");
                Assert.Equal(0, productsCategory.References.Count);
                Assert.Equal(1, tables.First(x => x.TableName == "category").References.Count);
                Assert.Equal(2, tables.First(x => x.TableName == "product").References.Count);
            }
        }
    }
}
