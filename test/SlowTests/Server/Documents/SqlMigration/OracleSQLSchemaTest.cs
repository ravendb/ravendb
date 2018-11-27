using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using Raven.Server.ServerWide.Context;
using Raven.Server.SqlMigration;
using Raven.Server.SqlMigration.Model;
using Raven.Server.SqlMigration.Schema;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.SqlMigration
{
    public class OracleClientSchemaTest : SqlAwareTestBase
    {
        [RequiresOracleSqlFact]
        public void CanFetchSchema()
        {
            using (WithSqlDatabase(MigrationProvider.OracleClient, out var connectionString, out string schemaName, includeData: false))
            {
                var driver = DatabaseDriverDispatcher.CreateDriver(MigrationProvider.OracleClient, connectionString);
                var schema = driver.FindSchema();

                Assert.NotNull(schema.CatalogName);
                Assert.Equal(10, schema.Tables.Count);

                var tables = schema.Tables;

                // validate NoPkTable
                var noPkTable = tables.First(x => x.TableName == "NOPKTABLE");
                Assert.NotNull(noPkTable);

                Assert.Equal(new[] { "ID" }, noPkTable.Columns.Select(x => x.Name).ToList());
                Assert.Equal(0, noPkTable.PrimaryKeyColumns.Count);

                // validate Order Table
                var orderTable = tables.First(x => x.TableName == "Order");

                Assert.Equal(4, orderTable.Columns.Count);
                Assert.Equal(new[] { "ID", "ORDERDATE", "CUSTOMERID", "TOTALAMOUNT" }, orderTable.Columns.Select(x => x.Name).ToList());
                Assert.Equal(new[] { "ID" }, orderTable.PrimaryKeyColumns);

                var orderReferences = orderTable.References;
                Assert.Equal(1, orderReferences.Count);
                Assert.Equal("ORDERITEM", orderReferences[0].Table);
                Assert.Equal(new[] { "ORDERID" }, orderReferences[0].Columns);

                // validate UnsupportedTable

                var unsupportedTable = tables.First(x => x.TableName == "UNSUPPORTEDTABLE");
                Assert.True(unsupportedTable.Columns.Any(x => x.Type == ColumnType.Unsupported));

                // validate OrderItem (2 columns in PK)

                var orderItemTable = tables.First(x => x.TableName == "ORDERITEM");
                Assert.Equal(new[] { "ORDERID", "PRODUCTID" }, orderItemTable.PrimaryKeyColumns);

                Assert.Equal(1, orderItemTable.References.Count);
                Assert.Equal("DETAILS", orderItemTable.References[0].Table);
                Assert.Equal(new[] { "ORDERID", "PRODUCTID" }, orderItemTable.References[0].Columns);

                // all types are supported (except UnsupportedTable)
                Assert.True(tables.Where(x => x.TableName != "UNSUPPORTEDTABLE")
                    .All(x => x.Columns.All(y => y.Type != ColumnType.Unsupported)));

                // validate many - to - many
                var productsCategory = tables.First(x => x.TableName == "PRODUCTCATEGORY");
                Assert.Equal(0, productsCategory.References.Count);
                Assert.Equal(1, tables.First(x => x.TableName == "CATEGORY").References.Count);
                Assert.Equal(2, tables.First(x => x.TableName == "PRODUCT").References.Count);
            }
        }
    }
}
