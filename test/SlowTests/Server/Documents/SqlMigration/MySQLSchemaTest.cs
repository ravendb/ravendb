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
    public class MySQLSchemaTest : SqlAwareTestBase
    {
        [Fact]
        public void CanFetchSchema()
        {
            using (WithSqlDatabase(MigrationProvider.MySQL, out var connectionString, includeData: false))
            {
                var driver = DatabaseDriverDispatcher.CreateDriver(MigrationProvider.MySQL, connectionString);
                var schema = driver.FindSchema();
                Assert.NotNull(schema.Name);

                Assert.Equal(21, schema.Tables.Count);

                // validate NoPkTable

                var tables = schema.Tables;

                var noPkTable = tables.First(x => x.Key == "nopktable");
                Assert.NotNull(noPkTable);

                Assert.Equal(new[] {"id"}, noPkTable.Value.Columns.Select(x => x.Name).ToList());
                Assert.Equal(0, noPkTable.Value.PrimaryKeyColumns.Count);

                // validate Order Table
                var orderTable = tables.First(x => x.Key == "orders");

                Assert.Equal(20, orderTable.Value.Columns.Count);
                Assert.Equal(new[] {"id"}, orderTable.Value.PrimaryKeyColumns);

                var orderReferences = orderTable.Value.References;
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
                var employeePrivileges = tables.First(x => x.Key == "employee_privileges");
                Assert.Equal(new[] {"employee_id", "privilege_id"}, employeePrivileges.Value.PrimaryKeyColumns);

                Assert.True(tables.All(x => x.Value.Columns.All(y => y.Type != ColumnType.Unsupported)));
            }
        }
    }
}
