using Raven.Server.Documents.ETL.Providers.SQL.RelationalWriters;
using Raven.Server.Documents.ETL.Providers.SQL;
using Raven.Server.ServerWide.Context;
using Raven.Server.SqlMigration;
using SlowTests.Server.Documents.Migration;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.SQL;
using Xunit;
using Xunit.Abstractions;
using System.Threading;
using System;
using Tests.Infrastructure;
using Npgsql;
using Raven.Server.Documents.ETL.Relational;
using Raven.Server.Documents.ETL.Relational.Test;

namespace SlowTests.Server.Documents.ETL.SQL;

public class RavenDB_19518 : SqlAwareTestBase
{
    public RavenDB_19518(ITestOutputHelper output) : base(output)
    {
    }

    private class Item
    {
        public string Id { get; set; }

        public string[] Companies { get; set; }
    }

    [RavenTheory(RavenTestCategory.Etl)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public async Task CanTestSqlEtlScriptWithPostgresSpecificTypes(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            const string docId = "items/1-A";
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Item
                {
                    Companies = new[] { "DHL", "UPS" },
                }, docId);
                await session.SaveChangesAsync();
            }

            var result1 = store.Maintenance.Send(new PutConnectionStringOperation<SqlConnectionString>(new SqlConnectionString()
            {
                Name = "simulate",
                ConnectionString = "Server=127.0.0.1;Port=2345;Database=myDataBase;User Id=foo;Password=bar;",
                FactoryName = "Npgsql",
            }));

            Assert.NotNull(result1.RaftCommandIndex);

            var database = await Etl.GetDatabaseFor(store, docId);

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var testResult = SqlEtl.TestScript(
                    new TestRelationalEtlScript<SqlConnectionString, SqlEtlConfiguration>
                    {
                        PerformRolledBackTransaction = false,
                        DocumentId = docId,
                        Configuration = new SqlEtlConfiguration()
                        {
                            Name = "simulate",
                            ConnectionStringName = "simulate",
                            SqlTables = { new SqlEtlTable { TableName = "Items", DocumentIdColumn = "Id" } },
                            Transforms =
                            {
                                new Transformation()
                                {
                                    Collections = { "Items" },
                                    Name = "Items",
                                    Script = @"
var data = {
    Companies:  {
        Type : 'Array | Text',
        Value : this.Companies.map(function(l) {return l;}),
    }
};
loadToItems(data);"
                                }
                            }
                        }
                    }, database, database.ServerStore, context);
                
                var result = (RelationalDatabaseEtlTestScriptResult)testResult;
                Assert.Equal(0, result.TransformationErrors.Count);
                Assert.Equal(0, result.LoadErrors.Count);
                Assert.Equal(0, result.SlowSqlWarnings.Count);

                Assert.Equal(1, result.Summary.Count);

                var orderLines = result.Summary.First(x => x.TableName == "Items");

                Assert.Equal(2, orderLines.Commands.Length); // delete and insert
            }
        }
    }

    private const string DefaultScript = @"
var orderData = {
    Id: documentId,
    OrderLinesCount: this.OrderLines.length,
    TotalCost: 0,
    Quantities: { 
        Type : 'Array | Double',
        Value : this.OrderLines.map(function(l) {return l.Quantity;})
    },
    Products: { 
        Type : 'Array | Text',
        Value : this.OrderLines.map(function(l) {return l.Product;})
    },
};
loadToorders(orderData);

for (var i = 0; i < this.OrderLines.length; i++) {
    var line = this.OrderLines[i];
    orderData.TotalCost += line.Cost;
    loadToorder_lines({
        OrderId: documentId,
        Qty: line.Quantity,
        Product: line.Product,
        Cost: line.Cost
    });
}";
    
    [RequiresNpgSqlFact]
    public void CanReplicateToArraysInPostgresSQL()
    {
        MigrationProvider provider = MigrationProvider.NpgSQL;
        using (var store = GetDocumentStore())
        {
            using (WithSqlDatabase(provider, out var connectionString, out string schemaName, dataSet: null, includeData: false))
            {
                CreateRdbmsSchema(connectionString);

                var sqlEtlConfigurationName = "OrdersAndLines_" + Guid.NewGuid();
                var connectionStringName = $"OrdersAndLines_{store.Database}";
                var operation = new PutConnectionStringOperation<SqlConnectionString>(new SqlConnectionString
                {
                    Name = connectionStringName,
                    FactoryName = @"Npgsql",
                    ConnectionString = connectionString
                });

                store.Maintenance.Send(operation);

                var etlDone = new ManualResetEventSlim();
                var configuration = new SqlEtlConfiguration
                {
                    Name = sqlEtlConfigurationName,
                    ConnectionStringName = connectionStringName,
                    SqlTables = { new SqlEtlTable { TableName = "orders", DocumentIdColumn = "Id" }, new SqlEtlTable { TableName = "order_lines", DocumentIdColumn = "OrderId" } },
                    Transforms =
                        {
                            new Transformation
                            {
                                Name = "OrdersAndLines",
                                Collections = {"Orders"},
                                Script = DefaultScript
                            },
                        }
                };

                store.Maintenance.Send(new AddEtlOperation<SqlConnectionString>(configuration));
                var database = GetDatabase(store.Database).Result;
                var errors = 0;
                database.EtlLoader.BatchCompleted += x =>
                {
                    if (x.ConfigurationName == sqlEtlConfigurationName && x.TransformationName == "OrdersAndLines")
                    {
                        if (x.Statistics.LoadSuccesses > 0)
                        {
                            errors = x.Statistics.LoadErrors;
                            etlDone.Set();
                        }
                    }
                };

                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        OrderLines = new List<OrderLine>
                        {
                            new OrderLine{Cost = 3, Product = "Milk", Quantity = 3},
                            new OrderLine{Cost = 4, Product = "Bear", Quantity = 2},
                        }
                    });
                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));
                Assert.Equal(0, errors);

                AssertCounts(connectionString, 1, 2, new[] { 3, 2 }, new[] { "Milk", "Bear" });
            }
        }
    }

    private void CreateRdbmsSchema(string connectionString)
    {
        using (var con = new NpgsqlConnection(connectionString))
        {
            con.Open();

            using (var dbCommand = con.CreateCommand())
            {
                dbCommand.CommandText = @"

DROP TABLE IF EXISTS order_lines;
DROP TABLE IF EXISTS orders;
";
                dbCommand.ExecuteNonQuery();

                dbCommand.CommandText = @"
CREATE TABLE order_lines
(
    ""Id"" serial primary key,
    ""OrderId"" text NOT NULL,
    ""Qty"" int NOT NULL,
    ""Product"" text NOT NULL,
    ""Cost"" int NOT NULL
);

CREATE TABLE orders
(
    ""Id"" text NOT NULL,
    ""OrderLinesCount"" int  NULL,
    ""TotalCost"" int NOT NULL,
    ""City"" text NULL,
    ""Quantities"" int[] NULL,
    ""Products"" text[] NULL
);";
                dbCommand.ExecuteNonQuery();
            }
        }
    }

    private static void AssertCounts(string connectionString, long ordersCount, long orderLineCounts, int[] orderQuantities = null, string[] products = null)
    {
        using (var con = new NpgsqlConnection(connectionString))
        {
            con.Open();
            using (var dbCommand = con.CreateCommand())
            {
                dbCommand.CommandText = "SELECT COUNT(*) FROM orders";
                Assert.Equal(ordersCount, dbCommand.ExecuteScalar());
                dbCommand.CommandText = "SELECT COUNT(*) FROM order_lines";
                Assert.Equal(orderLineCounts, dbCommand.ExecuteScalar());

                if (orderQuantities != null)
                {
                    dbCommand.CommandText = "SELECT \"Quantities\" FROM orders LIMIT 1";
                    var quantities = dbCommand.ExecuteScalar();
                    Assert.Equal(orderQuantities, (int[])quantities);
                }

                if (products != null)
                {
                    dbCommand.CommandText = "SELECT \"Products\" FROM orders LIMIT 1";
                    var productNames = dbCommand.ExecuteScalar();
                    Assert.Equal(products, (string[])productNames);
                }
            }
        }
    }


    private class Order
    {
        public Address Address { get; set; }
        public string Id { get; set; }
        public List<OrderLine> OrderLines { get; set; }
    }

    private class Address
    {
        public string City { get; set; }
    }

    private class OrderLine
    {
        public string Product { get; set; }
        public int Quantity { get; set; }
        public int Cost { get; set; }
    }
}
