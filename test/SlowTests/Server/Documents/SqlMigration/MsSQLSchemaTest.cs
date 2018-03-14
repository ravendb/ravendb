using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.SqlMigration;
using Raven.Server.SqlMigration.MsSQL;
using Raven.Server.SqlMigration.Schema;
using SlowTests.Server.Documents.ETL.SQL;
using Xunit;

namespace SlowTests.Server.Documents.SqlMigration
{
    public class MsSQLSchemaTest : SqlAwareTestBase
    {
        [Fact]
        public void CanFetchSchema()
        {
            using (WithSqlDatabase(MigrationProvider.MsSQL, out var connectionString, includeData: false))
            {
                var driver = DatabaseDriverDispatcher.CreateDriver(MigrationProvider.MsSQL, connectionString);
                var schema = driver.FindSchema();

                Assert.NotNull(schema.Name);
                Assert.Equal(10, schema.Tables.Count);

                // validate NoPkTable

                var tables = schema.Tables;

                var noPkTable = tables.First(x => x.Key == "NoPkTable");
                Assert.NotNull(noPkTable);

                Assert.Equal(new[] {"Id"}, noPkTable.Value.Columns.Select(x => x.Name).ToList());
                Assert.Equal(0, noPkTable.Value.PrimaryKeyColumns.Count);

                // validate Order Table
                var orderTable = tables.First(x => x.Key == "Order");

                Assert.Equal(4, orderTable.Value.Columns.Count);
                Assert.Equal(new[] {"Id", "OrderDate", "CustomerId", "TotalAmount"}, orderTable.Value.Columns.Select(x => x.Name).ToList());
                Assert.Equal(new[] {"Id"}, orderTable.Value.PrimaryKeyColumns);

                var orderReferences = orderTable.Value.References;
                Assert.Equal(1, orderReferences.Count);
                Assert.Equal("OrderItem", orderReferences[0].Table);
                Assert.Equal(new[] {"OrderId"}, orderReferences[0].Columns);

                // validate UnsupportedTable

                var unsupportedTable = tables.First(x => x.Key == "UnsupportedTable");
                Assert.True(unsupportedTable.Value.Columns.Any(x => x.Type == ColumnType.Unsupported));

                // validate OrderItem (2 columns in PK)

                var orderItemTable = tables.First(x => x.Key == "OrderItem");
                Assert.Equal(new[] {"OrderId", "ProductId"}, orderItemTable.Value.PrimaryKeyColumns);

                Assert.Equal(1, orderItemTable.Value.References.Count);
                Assert.Equal("Details", orderItemTable.Value.References[0].Table);
                Assert.Equal(new[] {"OrderId", "ProductId"}, orderItemTable.Value.References[0].Columns);

                // all types are supported (except UnsupportedTable)
                Assert.True(tables.Where(x => x.Key != "UnsupportedTable")
                    .All(x => x.Value.Columns.All(y => y.Type != ColumnType.Unsupported)));

                // validate many - to - many
                var productsCategory = tables.First(x => x.Key == "ProductCategory");
                Assert.Equal(0, productsCategory.Value.References.Count);
                Assert.Equal(1, tables.First(x => x.Key == "Category").Value.References.Count);
                Assert.Equal(2, tables.First(x => x.Key == "Product").Value.References.Count);
            }
        }
    }
}
