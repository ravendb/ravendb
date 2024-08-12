using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using FastTests.Voron.Util;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.Snowflake;
using Raven.Client.Util;
using Snowflake.Data.Client;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using TestSnowflakeConnectionString = Tests.Infrastructure.ConnectionString.SnowflakeConnectionString;

namespace SlowTests.Server.Documents.ETL.Snowflake;

public class SnowflakeEtlTests(ITestOutputHelper output) : RavenTestBase(output)
{
    private const int CommandTimeout = 10; // We want to avoid timeout exception. We don't care about performance here. The query can take long time if all the outer database are working simultaneously on the same machine 

    protected const string DefaultScript = @"
var orderData = {
    Id: id(this),
    OrderLinesCount: this.OrderLines.length,
    TotalCost: 0
};

for (var i = 0; i < this.OrderLines.length; i++) {
    var line = this.OrderLines[i];
    orderData.TotalCost += line.Cost;
    loadToOrderLines({
        OrderId: id(this),
        Qty: line.Quantity,
        Product: line.Product,
        Cost: line.Cost
    });
}

loadToOrders(orderData);
";

    protected const string DefaultScriptRegionsTableWithTerritoriesArrayColumn = @"
loadToRegions({
    Id: id(this),
    Name: this.Name,
    Territories: {Type: 'Array', Value: this.Territories}
});
";
    
    
    private static DisposableAction WithSnowflakeDatabase(out string connectionString, out string databaseName, out string schemaName)
    {
        databaseName = "snowflake_test_" + Guid.NewGuid();
        schemaName = "snowflake_test_" + Guid.NewGuid();
        var rawConnectionString = TestSnowflakeConnectionString.Instance.VerifiedConnectionString.Value;
        
        if(string.IsNullOrEmpty(rawConnectionString))
            throw new InvalidOperationException("The connection string for Snowflake db is null");
        
        connectionString = $"{rawConnectionString}DB=\"{databaseName}\";schema=\"{schemaName}\";";

        using (var connection = new SnowflakeDbConnection(rawConnectionString))
        {
            connection.Open();

            using (var dbCommand = connection.CreateCommand())
            {
                dbCommand.CommandTimeout = CommandTimeout;
                dbCommand.CommandText = $"CREATE DATABASE \"{databaseName}\"";
                dbCommand.ExecuteNonQuery();
                dbCommand.CommandText = $"USE DATABASE \"{databaseName}\"";
                dbCommand.ExecuteNonQuery();
                dbCommand.CommandText = $"CREATE SCHEMA \"{schemaName}\"";
                dbCommand.ExecuteNonQuery();
            }
        }
        
        string dbName = databaseName;
        return new DisposableAction(() =>
        {
            using (var con = new SnowflakeDbConnection(rawConnectionString))
            {
                con.Open();

                using (var dbCommand = con.CreateCommand())
                {
                    dbCommand.CommandTimeout = CommandTimeout;
                    var dropDatabaseQuery = "DROP DATABASE \"{0}\"";
                    dbCommand.CommandText = string.Format(dropDatabaseQuery, dbName);

                    dbCommand.ExecuteNonQuery();
                }
            }
        });
    }
    
    private static void AssertCounts(int ordersCount, int orderLineCounts, string connectionString)
    {
        using (var con = new SnowflakeDbConnection())
        {
            con.ConnectionString = connectionString;
            con.Open();

            using (var dbCommand = con.CreateCommand())
            {
                dbCommand.CommandText = "SELECT COUNT(*) FROM OrderLines";
                var result = dbCommand.ExecuteScalar();
                Assert.Equal((long)orderLineCounts, result);
                
                dbCommand.CommandText = "SELECT COUNT(*) FROM Orders";
                result = dbCommand.ExecuteScalar();
                Assert.Equal((long)ordersCount, result);
            }
        }
    }
    
    private static void AssertCountsRegions(int regionsCount, string connectionString)
    {
        using (var con = new SnowflakeDbConnection())
        {
            con.ConnectionString = connectionString;
            con.Open();

            using (var dbCommand = con.CreateCommand())
            {
                dbCommand.CommandText = "SELECT COUNT(*) FROM Regions";
                var result = dbCommand.ExecuteScalar();
                Assert.Equal((long)regionsCount, result);
            }
        }
    }
    
    protected void SetupSnowflakeEtl(DocumentStore store, string connectionString, string script, bool insertOnly = false, List<string> collections = null)
    {
        var connectionStringName = $"{store.Database}@{store.Urls.First()} to Snowflake DB";

        Etl.AddEtl(store,
            new SnowflakeEtlConfiguration()
            {
                Name = connectionStringName,
                ConnectionStringName = connectionStringName,
                SnowflakeTables =
                {
                    new SnowflakeEtlTable { TableName = "Orders", DocumentIdColumn = "Id", InsertOnlyMode = insertOnly },
                    new SnowflakeEtlTable { TableName = "OrderLines", DocumentIdColumn = "OrderId", InsertOnlyMode = insertOnly },
                },
                Transforms = { new Transformation() { Name = "OrdersAndLines", Collections = collections ?? new List<string> { "Orders" }, Script = script } }
            }, new SnowflakeConnectionString { Name = connectionStringName, ConnectionString = connectionString, });
    }

    protected void SetupComplexDataSnowflakeEtl(DocumentStore store, string connectionString, string script, bool insertOnly = false, List<string> collections = null)
    {
        var connectionStringName = $"{store.Database}@{store.Urls.First()} to Snowflake DB";
        
        Etl.AddEtl(store,
            new SnowflakeEtlConfiguration()
            {
                Name = $"Regions{connectionStringName}",
                ConnectionStringName = connectionStringName,
                SnowflakeTables = { new SnowflakeEtlTable() { TableName = "Regions", DocumentIdColumn = "Id", InsertOnlyMode = insertOnly } },
                Transforms = { new Transformation() { Name = "Regions", Collections = new List<string>() { "Regions" }, Script = DefaultScriptRegionsTableWithTerritoriesArrayColumn } }
            }, new SnowflakeConnectionString() { Name = connectionStringName, ConnectionString = connectionString });
    }

    protected static void CreateOrdersAndOrderLinesTables(string connectionString)
    {
        using (var con = new SnowflakeDbConnection())
        {
            con.ConnectionString = connectionString;
            con.Open();

            using (var dbCommand = con.CreateCommand())
            {
                dbCommand.CommandText = "create or replace TABLE ORDERS (\n\tID VARCHAR(50),\n\tORDERLINESCOUNT NUMBER(38,0),\n\tTOTALCOST NUMBER(38,0),\n\tCITY VARCHAR(50)\n);";
                dbCommand.ExecuteNonQuery();
                
                dbCommand.CommandText = "create or replace TABLE ORDERLINES (\n\tID VARCHAR(50),\n\tORDERID VARCHAR(50),\n\tQTY NUMBER(38,0),\n\tPRODUCT VARCHAR(255),\n\tCOST NUMBER(38,0)\n);";
                dbCommand.ExecuteNonQuery();
            }
        }
    }

    protected static void CreateRegionsTables(string connectionString)
    {
        using (var con = new SnowflakeDbConnection())
        {
            con.ConnectionString = connectionString;
            con.Open();

            using (var dbCommand = con.CreateCommand())
            {
                dbCommand.CommandText = "create or replace TABLE REGIONS (\n\tID STRING,\n\tNAME STRING,\n\tTERRITORIES ARRAY\n);";
                dbCommand.ExecuteNonQuery();
            }
        }
    }

    protected static void CreateOrdersWithAttachmentTable(string connectionString)
    {
        using (var con = new SnowflakeDbConnection())
        {
            con.ConnectionString = connectionString;
            con.Open();

            using (var dbCommand = con.CreateCommand())
            {
                dbCommand.CommandText = "create or replace TABLE ORDERS (\n\tID STRING,\n\tNAME STRING,\n\tPIC BINARY\n);";
                dbCommand.ExecuteNonQuery();
            }
        }
    }
    
    [RequiresSnowflakeFact]
    public async Task CanUseSnowflakeEtl()
    {
        using (var store = GetDocumentStore())
        {
            using (WithSnowflakeDatabase(out var connectionString, out string databaseName, out string schemaName))
            {
                CreateOrdersAndOrderLinesTables(connectionString);
                
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order
                    {
                        Id = "orders/1",
                        OrderLines = new List<OrderLine>
                        {
                            new OrderLine { Cost = 3, Product = "Milk", Quantity = 3 }, new OrderLine { Cost = 4, Product = "Bear", Quantity = 2 },
                        },
                    });
                    await session.SaveChangesAsync();
                }

                var etlDone = Etl.WaitForEtlToComplete(store);
                
                SetupSnowflakeEtl(store, connectionString, DefaultScript, insertOnly: true);

                etlDone.Wait(TimeSpan.FromSeconds(10));

                AssertCounts(1, 2, connectionString);

                etlDone.Reset();

                using (var session = store.OpenAsyncSession())
                {
                    var order = await session.LoadAsync<Order>("orders/1");
                    order.OrderLines.Add(new OrderLine { Cost = 5, Product = "Sugar", Quantity = 7 });
                    await session.SaveChangesAsync();
                }

                etlDone.Wait(TimeSpan.FromMinutes(5));
                // we end up with duplicates
                AssertCounts(2, 5, connectionString);
            }
        }
    }
    
    [RequiresSnowflakeFact]
    public async Task CanUseSnowflakeEtlForComplexData()
    {
        using (var store = GetDocumentStore())
        {
            using (WithSnowflakeDatabase(out var connectionString, out string databaseName, out string schemaName))
            {
                CreateRegionsTables(connectionString);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Region { Name= "Special", Territories = [new Territory { Code = "95060", Name = "Santa Cruz" }] });
                    await session.SaveChangesAsync();
                }

                var etlDone = Etl.WaitForEtlToComplete(store);

                SetupComplexDataSnowflakeEtl(store, connectionString, DefaultScriptRegionsTableWithTerritoriesArrayColumn, insertOnly: true);

                etlDone.Wait(TimeSpan.FromSeconds(10));
                
                AssertCountsRegions(1, connectionString);
            }
        }
    }
    
    
    [RequiresSnowflakeRetryTheory(delayBetweenRetriesMs: 1000)]
    [InlineData(RavenDatabaseMode.Single)]
    [InlineData(RavenDatabaseMode.Sharded)]
    public async Task ShouldHandleCaseMismatchBetweenTableDefinitionAndLoadTo(RavenDatabaseMode databaseMode)
    {
        using (var store = GetDocumentStore(Options.ForMode(databaseMode)))
        {
            using (WithSnowflakeDatabase(out var connectionString, out var _, out var _))
            {
                CreateOrdersAndOrderLinesTables(connectionString);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order
                    {
                        OrderLines = new List<OrderLine>
                        {
                            new OrderLine {Cost = 3, Product = "Milk", Quantity = 3}, new OrderLine {Cost = 4, Product = "Bear", Quantity = 2},
                        }
                    });
                    await session.SaveChangesAsync();
                }

                var etlDone = Etl.WaitForEtlToComplete(store);

                string script = @"
var orderData = {
Id: id(this),
OrderLinesCount: this.OrderLines.length,
TotalCost: 0
};

loadToOrDerS(orderData); // note 'OrDerS' here vs 'Orders' defined in the configuration
";

                SetupSnowflakeEtl(store, connectionString, script);

                etlDone.Wait(TimeSpan.FromMinutes(5));

                using (var con = new SnowflakeDbConnection())
                {
                    con.ConnectionString = connectionString;
                    con.Open();
                        
                    Assert.Equal(1, GetOrdersCount(connectionString));
                }
            }
        }
    }
    
    
    [RequiresSnowflakeRetryTheory(delayBetweenRetriesMs: 1000)]
    [InlineData(RavenDatabaseMode.Single)]
    [InlineData(RavenDatabaseMode.Sharded)]
    public async Task NullPropagation(RavenDatabaseMode databaseMode)
    {
        using (var store = GetDocumentStore(Options.ForMode(databaseMode)))
        {
            using (WithSnowflakeDatabase(out var connectionString, out var _, out var _))
            {
                CreateOrdersAndOrderLinesTables(connectionString);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order
                    {
                        OrderLines = new List<OrderLine>
                        {
                            new OrderLine {Cost = 3, Product = "Milk", Quantity = 3}, new OrderLine {Cost = 4, Product = "Bear", Quantity = 2},
                        }
                    });
                    await session.SaveChangesAsync();
                }

                var etlDone = Etl.WaitForEtlToComplete(store);

                SetupSnowflakeEtl(store, connectionString, @"var orderData = {
Id: id(this),
OrderLinesCount: this.OrderLines_Missing.length,
TotalCost: 0
};
loadToOrders(orderData);");

                etlDone.Wait(TimeSpan.FromMinutes(5));

                using (var con = new SnowflakeDbConnection())
                {
                    con.ConnectionString = connectionString;
                    con.Open();
                    AssertCounts(1, 0, connectionString);
                }
            }
        }
    }
    
    [RequiresSnowflakeRetryTheory(delayBetweenRetriesMs: 1000)]
    [InlineData(RavenDatabaseMode.Single)]
    [InlineData(RavenDatabaseMode.Sharded)]
    public async Task NullPropagation_WithExplicitNull(RavenDatabaseMode databaseMode)
    {
        using (var store = GetDocumentStore(Options.ForMode(databaseMode)))
        {
            using (WithSnowflakeDatabase(out var connectionString, out var _, out var _))
            {
                CreateOrdersAndOrderLinesTables(connectionString);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order
                    {
                        Address = null,
                        OrderLines = new List<OrderLine>
                        {
                            new OrderLine {Cost = 3, Product = "Milk", Quantity = 3}, new OrderLine {Cost = 4, Product = "Beer", Quantity = 2},
                        }
                    });
                    await session.SaveChangesAsync();
                }

                var etlDone = Etl.WaitForEtlToComplete(store);

                SetupSnowflakeEtl(store, connectionString, @"var orderData = {
Id: id(this),
City: this.Address.City,
TotalCost: 0
};
loadToOrders(orderData);");

                etlDone.Wait(TimeSpan.FromMinutes(5));

                using (var con = new SnowflakeDbConnection())
                {
                    con.ConnectionString = connectionString;
                    con.Open();

                    using (var dbCommand = con.CreateCommand())
                    {
                        dbCommand.CommandText = " SELECT COUNT(*) FROM Orders";
                        Assert.Equal(1L, dbCommand.ExecuteScalar());
                        dbCommand.CommandText = " SELECT City FROM Orders";
                        Assert.Equal(DBNull.Value, dbCommand.ExecuteScalar());
                    }
                }
            }
        }
    }
    
    [RequiresMsSqlRetryTheory(delayBetweenRetriesMs: 1000)]
    [InlineData(RavenDatabaseMode.Single)]
    [InlineData(RavenDatabaseMode.Sharded)]
    public async Task RavenDB_3341(RavenDatabaseMode databaseMode)
    {
        using (var store = GetDocumentStore(Options.ForMode(databaseMode)))
        {
            using (WithSnowflakeDatabase(out var connectionString, out var _, out var _))
            {
                CreateOrdersAndOrderLinesTables(connectionString);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order { OrderLines = new List<OrderLine> { new OrderLine { Cost = 4, Product = "Bear", Quantity = 2 }, } });
                    await session.SaveChangesAsync();
                }

                var etlDone = Etl.WaitForEtlToComplete(store);

                SetupSnowflakeEtl(store, connectionString, "if(this.OrderLines.length > 0) { \r\n" + DefaultScript + " \r\n}");

                etlDone.Wait(TimeSpan.FromMinutes(5));

                AssertCounts(1, 1, connectionString);

                etlDone.Reset();
                using (var session = store.OpenAsyncSession())
                {
                    var order = await session.LoadAsync<Order>("orders/1-A");
                    order.OrderLines.Clear();
                    await session.SaveChangesAsync();
                }

                etlDone.Wait(TimeSpan.FromMinutes(5));
                AssertCounts(0, 0, connectionString);
            }
        }
    }
    
    [RequiresMsSqlRetryTheory(delayBetweenRetriesMs: 1000)]
    [InlineData(RavenDatabaseMode.Single)]
    [InlineData(RavenDatabaseMode.Sharded)]
    public async Task CanUpdateToBeNoItemsInChildTable(RavenDatabaseMode databaseMode)
    {
        using (var store = GetDocumentStore(Options.ForMode(databaseMode)))
        {
            using (WithSnowflakeDatabase(out var connectionString, out var _, out var _))
            {
                CreateOrdersAndOrderLinesTables(connectionString);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order
                    {
                        OrderLines = new List<OrderLine>
                        {
                            new OrderLine {Cost = 3, Product = "Milk", Quantity = 3}, new OrderLine {Cost = 4, Product = "Bear", Quantity = 2},
                        }
                    });
                    await session.SaveChangesAsync();
                }

                var etlDone = Etl.WaitForEtlToComplete(store);

                SetupSnowflakeEtl(store, connectionString, DefaultScript);

                etlDone.Wait(TimeSpan.FromMinutes(5));

                AssertCounts(1, 2, connectionString);

                etlDone.Reset();

                using (var session = store.OpenAsyncSession())
                {
                    var order = await session.LoadAsync<Order>("orders/1-A");
                    order.OrderLines.Clear();
                    await session.SaveChangesAsync();
                }

                etlDone.Wait(TimeSpan.FromMinutes(5));
                AssertCounts(1, 0, connectionString);
            }
        }
    }
    
    [RequiresSnowflakeFact]
    public async Task CanLoadSingleAttachment()
    {
        using (var store = GetDocumentStore())
        {
            using (WithSnowflakeDatabase(out var connectionString, out string databaseName, out string schemaName))
            {
                CreateOrdersWithAttachmentTable(connectionString);
                var attachmentBytes = new byte[] { 1, 2, 3 };
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order());
                    await session.SaveChangesAsync();
                }

                store.Operations.Send(new PutAttachmentOperation("orders/1-A", "test-attachment", new MemoryStream(attachmentBytes), "image/png"));

                var etlDone = Etl.WaitForEtlToComplete(store);

                SetupSnowflakeEtl(store, connectionString, @"
var orderData = {
    Id: id(this),
    Name: this['@metadata']['@attachments'][0].Name,
    Pic: loadAttachment(this['@metadata']['@attachments'][0].Name)
};

loadToOrders(orderData);
");
                etlDone.Wait(TimeSpan.FromMinutes(5));
                using (var con = new SnowflakeDbConnection())
                {
                    con.ConnectionString = connectionString;
                    con.Open();

                    Assert.Equal(1L, GetOrdersCount(connectionString));

                    using (var dbCommand = con.CreateCommand())
                    {
                        dbCommand.CommandText = " SELECT Pic FROM Orders WHERE Id = 'orders/1-A'";

                        var sqlDataReader = dbCommand.ExecuteReader();

                        Assert.True(sqlDataReader.Read());
                        var stream = sqlDataReader.GetStream(0);

                        var bytes = stream.ReadData();

                        Assert.Equal(attachmentBytes, bytes);
                    }
                }
            }
        }
    }

    internal static long GetOrdersCount(string connectionString)
    {
        using (var con = new SnowflakeDbConnection())
        {
            con.ConnectionString = connectionString;
            con.Open();

            using (var dbCommand = con.CreateCommand())
            {
                dbCommand.CommandText = " SELECT COUNT(*) FROM Orders";
                return (long)dbCommand.ExecuteScalar();
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

    private class FavouriteOrder : Order
    {

    }

}
