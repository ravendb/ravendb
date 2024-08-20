using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using FastTests.Voron.Util;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.Snowflake;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Extensions;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Documents.ETL.Providers.RelationalDatabase.Common;
using Raven.Server.Documents.ETL.Providers.RelationalDatabase.Common.Test;
using Raven.Server.Documents.ETL.Providers.RelationalDatabase.Snowflake;
using Raven.Server.ServerWide.Context;
using SlowTests.Core.Utils.Entities;
using Snowflake.Data.Client;
using Sparrow.Server;
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
        CreateSnowflakeTable(connectionString,
            "create or replace TABLE ORDERS (\n\tID VARCHAR(50),\n\tORDERLINESCOUNT NUMBER(38,0),\n\tTOTALCOST NUMBER(38,0),\n\tCITY VARCHAR(50)\n);");
        CreateSnowflakeTable(connectionString,
            "create or replace TABLE ORDERLINES (\n\tID VARCHAR(50),\n\tORDERID VARCHAR(50),\n\tQTY NUMBER(38,0),\n\tPRODUCT VARCHAR(255),\n\tCOST NUMBER(38,0)\n);");
    }

    protected static void CreateSnowflakeTable(string connectionString, string commandString)
    {
        using (var con = new SnowflakeDbConnection())
        {
            con.ConnectionString = connectionString;
            con.Open();

            using (var dbCommand = con.CreateCommand())
            {
                dbCommand.CommandText = commandString;
                dbCommand.ExecuteNonQuery();
            }
        }
    }
    
    protected static void CreateOrdersWithAttachmentTable(string connectionString)
    {
        CreateSnowflakeTable(connectionString, "create or replace TABLE ORDERS (\n\tID STRING,\n\tNAME STRING,\n\tPIC BINARY\n);");
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
    public async Task CanUseSnowflakeEtlForArrayData()
    {
        using (var store = GetDocumentStore())
        {
            using (WithSnowflakeDatabase(out var connectionString, out string databaseName, out string schemaName))
            {
                CreateSnowflakeTable(connectionString, "create or replace TABLE REGIONS (\n\tID STRING,\n\tNAME STRING,\n\tTERRITORIES ARRAY\n);");
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
    
    
    [RequiresMsSqlRetryTheory(delayBetweenRetriesMs: 1000)]
    [InlineData(RavenDatabaseMode.Single)]
    [InlineData(RavenDatabaseMode.Sharded)]
    public async Task RavenDB_3172(RavenDatabaseMode databaseMode)
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
                        Id = "orders/1",
                        OrderLines = new List<OrderLine>
                        {
                            new OrderLine {Cost = 3, Product = "Milk", Quantity = 3}, new OrderLine {Cost = 4, Product = "Bear", Quantity = 2},
                        }
                    });
                    await session.SaveChangesAsync();
                }

                var etlDone = Etl.WaitForEtlToComplete(store);

                SetupSnowflakeEtl(store, connectionString, DefaultScript, insertOnly: true);

                etlDone.Wait(TimeSpan.FromMinutes(5));

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
    
    [RequiresMsSqlRetryTheory(delayBetweenRetriesMs: 1000)]
    [InlineData(RavenDatabaseMode.Single)]
    [InlineData(RavenDatabaseMode.Sharded)]
    public async Task WillLog(RavenDatabaseMode databaseMode)
    {
        using (var client = new ClientWebSocket())
        using (var store = GetDocumentStore(Options.ForMode(databaseMode)))
        {
            using (WithSnowflakeDatabase(out var connectionString, out var _, out var _))
            {
                CreateOrdersAndOrderLinesTables(connectionString);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order());
                    await session.SaveChangesAsync();
                }

                var str = string.Format("{0}/admin/logs/watch", store.Urls.First().Replace("http", "ws"));
                var sb = new StringBuilder();

                var mre = new AsyncManualResetEvent();

                await client.ConnectAsync(new Uri(str), CancellationToken.None);
                var task = Task.Run(async () =>
                {
                    ArraySegment<byte> buffer = new ArraySegment<byte>(new byte[1024]);
                    while (client.State == WebSocketState.Open)
                    {
                        var value = await ReadFromWebSocket(buffer, client);
                        lock (sb)
                        {
                            mre.Set();
                            sb.AppendLine(value);
                        }

                        const string expectedValue = "skipping document: orders/";
                        if (value.Contains(expectedValue) || sb.ToString().Contains(expectedValue))
                            return;

                    }
                });
                await mre.WaitAsync(TimeSpan.FromSeconds(60));
                SetupSnowflakeEtl(store, connectionString, @"output ('Tralala'); 

undefined();

var nameArr = this.StepName.split('.'); loadToOrders({});");

                using (var session = store.OpenAsyncSession())
                {
                    for (var i = 0; i < 100; i++)
                        await session.StoreAsync(new Order());

                    await session.SaveChangesAsync();
                }

                var condition = await task.WaitWithTimeout(TimeSpan.FromSeconds(60));
                if (condition == false)
                {
                    var msg = "Could not process Snowflake Replication script for OrdersAndLines, skipping document: orders/";
                    var tempFileName = Path.GetTempFileName();
                    lock (sb)
                    {
                        File.WriteAllText(tempFileName, sb.ToString());
                    }

                    throw new InvalidOperationException($"{msg}. Full log is: \r\n{tempFileName}");
                }
            }
        }
    }
    
    
    [RequiresMsSqlRetryTheory(delayBetweenRetriesMs: 1000)]
    [InlineData(RavenDatabaseMode.Single, true)]
    [InlineData(RavenDatabaseMode.Single, false)]
    [InlineData(RavenDatabaseMode.Sharded, true)]
    [InlineData(RavenDatabaseMode.Sharded, false)]

    public async Task CanTestScript(RavenDatabaseMode databaseMode, bool performRolledBackTransaction)
    {
        using (var store = GetDocumentStore(Options.ForMode(databaseMode)))
        {
            using (WithSnowflakeDatabase(out var connectionString, out var _, out var _))
            {
                CreateOrdersAndOrderLinesTables(connectionString);
                const string docId = "orders/1-A";
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order
                    {
                        OrderLines = new List<OrderLine>
                        {
                            new OrderLine {Cost = 3, Product = "Milk", Quantity = 3}, new OrderLine {Cost = 4, Product = "Bear", Quantity = 2},
                        }
                    }, docId);
                    await session.SaveChangesAsync();
                }

                var result1 = store.Maintenance.Send(new PutConnectionStringOperation<SnowflakeConnectionString>(new SnowflakeConnectionString()
                {
                    Name = "simulate",
                    ConnectionString = connectionString,
                }));
                Assert.NotNull(result1.RaftCommandIndex);
                Thread.Sleep(5000);
                
                var database = await Etl.GetDatabaseFor(store, docId);

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                {
                    var testResult = SnowflakeEtl.TestScript(
                        new TestRelationalDatabaseEtlScript<SnowflakeConnectionString, SnowflakeEtlConfiguration>
                        {
                            PerformRolledBackTransaction = performRolledBackTransaction,
                            DocumentId = docId,
                            Configuration = new SnowflakeEtlConfiguration()
                            {
                                Name = "simulate",
                                ConnectionStringName = "simulate",
                                SnowflakeTables =
                                {
                                    new SnowflakeEtlTable { TableName = "Orders", DocumentIdColumn = "Id" },
                                    new SnowflakeEtlTable { TableName = "OrderLines", DocumentIdColumn = "OrderId" },
                                    new SnowflakeEtlTable { TableName = "NotUsedInScript", DocumentIdColumn = "OrderId" },
                                },
                                Transforms =
                                {
                                    new Transformation()
                                    {
                                        Collections = { "Orders" }, Name = "OrdersAndLines", Script = DefaultScript + "output('test output')"
                                    }
                                }
                            }
                        }, database, database.ServerStore, context);
                    
                    var result = (RelationalDatabaseEtlTestScriptResult)testResult;
                    Assert.Equal(0, result.TransformationErrors.Count);
                    Assert.Equal(0, result.LoadErrors.Count);
                    Assert.Equal(0, result.SlowSqlWarnings.Count);

                    Assert.Equal(2, result.Summary.Count);

                    var orderLines = result.Summary.First(x => x.TableName == "OrderLines");

                    Assert.Equal(3, orderLines.Commands.Length); // delete and two inserts

                    var orders = result.Summary.First(x => x.TableName == "Orders");

                    Assert.Equal(2, orders.Commands.Length); // delete and insert

                    Assert.Equal("test output", result.DebugOutput[0]);
                }
            }
        }
    }
    
    
    [RequiresMsSqlRetryTheory(delayBetweenRetriesMs: 1000)]
    [InlineData(RavenDatabaseMode.Single)]
    [InlineData(RavenDatabaseMode.Sharded)]
    public async Task VarcharAndNVarcharFunctionsArentAvailable(RavenDatabaseMode databaseMode)
    {
        using (var store = GetDocumentStore(Options.ForMode(databaseMode)))
        {
            using (WithSnowflakeDatabase(out var connectionString, out var _, out var _))
            {
                CreateSnowflakeTable(connectionString,
                    "create or replace table USERS (ID VARCHAR(50), FIRSTNAME VARCHAR(30), LASTNAME VARCHAR(30), FIRSTNAME2 NVARCHAR(30), LASTNAME2 NVARCHAR(30));");

                const string docId = "users/1-A";
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Joe Doń" });

                    await session.SaveChangesAsync();
                }
                
                var result1 = store.Maintenance.Send(new PutConnectionStringOperation<SnowflakeConnectionString>(new SnowflakeConnectionString
                {
                    Name = "simulate",
                    ConnectionString = connectionString,
                }));
                Assert.NotNull(result1.RaftCommandIndex);

                var database = await Etl.GetDatabaseFor(store, docId);

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                {
                    var testResult = SnowflakeEtl.TestScript(
                        new TestRelationalDatabaseEtlScript<SnowflakeConnectionString, SnowflakeEtlConfiguration>
                        {
                            PerformRolledBackTransaction = true,
                            DocumentId = docId,
                            IsDelete = false,
                            Configuration = new SnowflakeEtlConfiguration()
                            {
                                Name = "CannotUseVarcharAndNVarcharFunctions",
                                ConnectionStringName = "simulate",
                                SnowflakeTables = { new SnowflakeEtlTable { TableName = "Users", DocumentIdColumn = "Id", InsertOnlyMode = false }, },
                                Transforms = 
                                {
                                    new Transformation 
                                    {
                                        Name = "varchartest",
                                        Collections = {"Users"},
                                        Script = @"
var names = this.Name.split(' ');

loadToUsers(
{
    FirstName: varchar(names[0], 30),
    LastName: varchar(names[1], 30),
    FirstName2: nvarchar(names[0]),
    LastName2:  nvarchar(names[1]),
});

// Checking if varchar and nvarchar functions exist in the current scope
let varcharExists = typeof varchar === 'function';
let nvarcharExists = typeof nvarchar === 'function';

// Creating an object to store the result
let result = {
    varcharExists: varcharExists,
    nvarcharExists: nvarcharExists
};

// Outputting the result
output(result);
"
                                    }   
                                }
                            }
                        }, database, database.ServerStore, context);
                    
                    var result = (RelationalDatabaseEtlTestScriptResult)testResult;
                    Assert.Equal(0, result.TransformationErrors.Count);
                    Assert.Equal(0, result.LoadErrors.Count);
                    Assert.Equal(0, result.SlowSqlWarnings.Count);

                    Assert.Equal(1, result.Summary.Count);

                    var users = result.Summary.First(x => x.TableName == "Users");

                    Assert.Equal(2, users.Commands.Length);  // insert & delete

                    Assert.Equal("{\"varcharExists\":false,\"nvarcharExists\":false}", result.DebugOutput[0]);
                }
            }
        }
    }
    
    [RequiresMsSqlRetryTheory(delayBetweenRetriesMs: 1000)]
    [InlineData(RavenDatabaseMode.Single, true)]
    [InlineData(RavenDatabaseMode.Single, false)]
    [InlineData(RavenDatabaseMode.Sharded, true)]
    [InlineData(RavenDatabaseMode.Sharded, false)]
    public async Task CanTestDeletion(RavenDatabaseMode databaseMode, bool performRolledBackTransaction)
    {
        using (var store = GetDocumentStore(Options.ForMode(databaseMode)))
        {
            using (WithSnowflakeDatabase(out var connectionString, out var _, out var _))
            {
                CreateOrdersAndOrderLinesTables(connectionString);

                const string docId = "orders/1-A";
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order
                    {
                        OrderLines = new List<OrderLine>
                        {
                            new OrderLine {Cost = 3, Product = "Milk", Quantity = 3}, new OrderLine {Cost = 4, Product = "Bear", Quantity = 2},
                        }
                    }, docId);
                    await session.SaveChangesAsync();
                }

                var result1 = store.Maintenance.Send(new PutConnectionStringOperation<SnowflakeConnectionString>(new SnowflakeConnectionString
                {
                    Name = "simulate",
                    ConnectionString = connectionString,
                }));
                Assert.NotNull(result1.RaftCommandIndex);

                var database = await Etl.GetDatabaseFor(store, docId);

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                {
                    var testResult = SnowflakeEtl.TestScript(
                        new TestRelationalDatabaseEtlScript<SnowflakeConnectionString, SnowflakeEtlConfiguration>
                        {
                            PerformRolledBackTransaction = performRolledBackTransaction,
                            DocumentId = docId,
                            IsDelete = true,
                            Configuration = new SnowflakeEtlConfiguration()
                            {
                                Name = "simulate",
                                ConnectionStringName = "simulate",
                                SnowflakeTables =
                                {
                                    new SnowflakeEtlTable { TableName = "Orders", DocumentIdColumn = "Id" },
                                    new SnowflakeEtlTable { TableName = "OrderLines", DocumentIdColumn = "OrderId" },
                                    new SnowflakeEtlTable { TableName = "NotUsedInScript", DocumentIdColumn = "OrderId" },
                                },
                                Transforms = { new Transformation() { Collections = { "Orders" }, Name = "OrdersAndLines", Script = DefaultScript } }
                            }
                        }, database, database.ServerStore, context);
                    
                    var result = (RelationalDatabaseEtlTestScriptResult)testResult;

                    Assert.Equal(0, result.TransformationErrors.Count);
                    Assert.Equal(0, result.LoadErrors.Count);
                    Assert.Equal(0, result.SlowSqlWarnings.Count);
                    Assert.Equal(2, result.Summary.Count);

                    var orderLines = result.Summary.First(x => x.TableName == "OrderLines");
                    Assert.Equal(1, orderLines.Commands.Length); // delete

                    var orders = result.Summary.First(x => x.TableName == "Orders");
                    Assert.Equal(1, orders.Commands.Length); // delete
                }

                using (var session = store.OpenAsyncSession())
                {
                    Assert.NotNull(session.Query<Order>(docId));
                }
            }
        }
    }

    [RequiresMsSqlRetryTheory(delayBetweenRetriesMs: 1000)]
    [InlineData(RavenDatabaseMode.Single)]
    [InlineData(RavenDatabaseMode.Sharded)]
    public async Task Should_not_error_if_attachment_doesnt_exist(RavenDatabaseMode databaseMode)
    {
        // the same test for sql should fail - snowflake accepts null as a binary value
        using (var store = GetDocumentStore(Options.ForMode(databaseMode)))
        {
            using (WithSnowflakeDatabase(out var connectionString, out var _, out var _))
            {
                CreateOrdersWithAttachmentTable(connectionString);

                var attachmentBytes = new byte[] { 1, 2, 3 };

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order(), "orders/1-A");
                    await session.StoreAsync(new Order(), "orders/2-A");
                    await session.StoreAsync(new Order(), "orders/3-A");

                    await session.SaveChangesAsync();
                }

                store.Operations.Send(new PutAttachmentOperation("orders/1-A", "abc.jpg", new MemoryStream(attachmentBytes), "image/png"));
                store.Operations.Send(new PutAttachmentOperation("orders/2-A", "photo.jpg", new MemoryStream(attachmentBytes), "image/png"));

                var etlDone = Etl.WaitForEtlToComplete(store, numOfProcessesToWaitFor: 2);

                SetupSnowflakeEtl(store, connectionString, @"
var orderData = {
    Id: id(this),
    Name: 'photo.jpg',
    Pic: loadAttachment('photo.jpg')
};

loadToOrders(orderData);
");

                etlDone.Wait(TimeSpan.FromMinutes(5));

                using (var con = new SnowflakeDbConnection())
                {
                    con.ConnectionString = connectionString;
                    con.Open();

                    using (var dbCommand = con.CreateCommand())
                    {
                        dbCommand.CommandText = " SELECT COUNT(*) FROM Orders";
                        Assert.Equal(3L, dbCommand.ExecuteScalar());
                    }

                    using (var dbCommand = con.CreateCommand())
                    {
                        dbCommand.CommandText = " SELECT Pic FROM Orders WHERE Id = 'orders/2-A'";

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
    
    [RequiresMsSqlRetryTheory(delayBetweenRetriesMs: 1000)]
    [InlineData(RavenDatabaseMode.Single)]
    [InlineData(RavenDatabaseMode.Sharded)]
    public async Task LoadingMultipleAttachments(RavenDatabaseMode databaseMode)
    {
        using (var store = GetDocumentStore(Options.ForMode(databaseMode)))
        {
            using (WithSnowflakeDatabase(out var connectionString, out var _, out var _))
            {
                CreateSnowflakeTable(connectionString,
                    "create or replace TABLE ATTACHMENTS (\n\tID STRING,\n\tUSERID STRING,\n\tATTACHMENTNAME VARCHAR(50),\n\tDATA BINARY\n);");

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User());
                    await session.SaveChangesAsync();
                }

                store.Operations.Send(new PutAttachmentOperation("users/1-A", "profile.jpg", new MemoryStream(new byte[] { 1, 2, 3, 4, 5, 6, 7 }), "image/jpeg"));
                store.Operations.Send(new PutAttachmentOperation("users/1-A", "profile-small.jpg", new MemoryStream(new byte[] { 1, 2, 3 }), "image/jpeg"));

                var etlDone = Etl.WaitForEtlToComplete(store);

                Etl.AddEtl(store, new SnowflakeEtlConfiguration()
                {
                    Name = "LoadingMultipleAttachments",
                    ConnectionStringName = "test",
                    SnowflakeTables = { new SnowflakeEtlTable { TableName = "Attachments", DocumentIdColumn = "UserId", InsertOnlyMode = false }, },
                    Transforms =
                    {
                        new Transformation()
                        {
                            Name = "Attachments",
                            Collections = {"Users"},
                            Script = @"

var attachments = this['@metadata']['@attachments'];

for (var i = 0; i < attachments.length; i++)
{
    var attachment = {
        UserId: id(this),
        AttachmentName: attachments[i].Name,
        Data: loadAttachment(attachments[i].Name)
    };

    loadToAttachments(attachment);
}
"
                        }
                    }
                }, new SnowflakeConnectionString() { Name = "test", ConnectionString = connectionString });

                etlDone.Wait(TimeSpan.FromMinutes(5));

                using (var con = new SnowflakeDbConnection())
                {
                    con.ConnectionString = connectionString;
                    con.Open();

                    using (var dbCommand = con.CreateCommand())
                    {
                        dbCommand.CommandText = " SELECT COUNT(*) FROM Attachments";
                        Assert.Equal(2L, dbCommand.ExecuteScalar());
                    }
                }
            }
        }
    }


    [RequiresMsSqlRetryTheory(delayBetweenRetriesMs: 1000)]
    [InlineData(RavenDatabaseMode.Single)]
    [InlineData(RavenDatabaseMode.Sharded)]
    public async Task CanSkipSettingFieldIfAttachmentDoesntExist(RavenDatabaseMode databaseMode)
    {
        using (var store = GetDocumentStore(Options.ForMode(databaseMode)))
        {
            using (WithSnowflakeDatabase(out var connectionString, out var _, out var _))
            {
                CreateSnowflakeTable(connectionString, "create or replace table Orders (ID VARCHAR(50), PIC BINARY);");
                
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order());
                    await session.SaveChangesAsync();
                }

                var etlDone = Etl.WaitForEtlToComplete(store);

                SetupSnowflakeEtl(store, connectionString, @"

var orderData = {
    Id: id(this),
    // Pic: loadAttachment('non-existing') // skip loading non existing attachment
};

loadToOrders(orderData);
");

                etlDone.Wait(TimeSpan.FromMinutes(5));

                using (var con = new SnowflakeDbConnection())
                {
                    con.ConnectionString = connectionString;
                    con.Open();

                    using (var dbCommand = con.CreateCommand())
                    {
                        dbCommand.CommandText = " SELECT COUNT(*) FROM Orders";
                        Assert.Equal(1L, dbCommand.ExecuteScalar());

                        dbCommand.CommandText = " SELECT Pic FROM Orders WHERE Id = 'orders/1-A'";

                        var sqlDataReader = dbCommand.ExecuteReader();

                        Assert.True(sqlDataReader.Read());
                        Assert.True(sqlDataReader.IsDBNull(0));
                    }
                }
            }
        }
    }
    
    [RequiresMsSqlRetryTheory(delayBetweenRetriesMs: 1000)]
    [InlineData(RavenDatabaseMode.Single)]
    [InlineData(RavenDatabaseMode.Sharded)]
    public async Task LoadingFromMultipleCollections(RavenDatabaseMode databaseMode)
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

                    await session.StoreAsync(new FavouriteOrder { OrderLines = new List<OrderLine> { new OrderLine { Cost = 3, Product = "Milk", Quantity = 3 }, } });

                    await session.SaveChangesAsync();
                }

                var etlDone = Etl.WaitForEtlToComplete(store, numOfProcessesToWaitFor: 2);

                SetupSnowflakeEtl(store, connectionString, DefaultScript, collections: new List<string> { "Orders", "FavouriteOrders" });

                etlDone.Wait(TimeSpan.FromMinutes(5));

                using (var con = new SnowflakeDbConnection())
                {
                    con.ConnectionString = connectionString;
                    con.Open();

                    using (var dbCommand = con.CreateCommand())
                    {
                        dbCommand.CommandText = " SELECT COUNT(*) FROM Orders";
                        Assert.Equal(2L, dbCommand.ExecuteScalar());
                        dbCommand.CommandText = " SELECT COUNT(*) FROM OrderLines";
                        Assert.Equal(3L, dbCommand.ExecuteScalar());
                    }
                }
            }
        }
    }
    
    [RequiresSnowflakeRetryFact(delayBetweenRetriesMs: 1000)]
    public void Should_stop_batch_if_size_limit_exceeded_RavenDB_12800()
    {
        using (var store = GetDocumentStore(new Options { ModifyDatabaseRecord = x => x.Settings[RavenConfiguration.GetKey(c => c.Etl.MaxBatchSize)] = "5" }))
        {
            using (WithSnowflakeDatabase(out var connectionString, out var _, out var _))
            {
                CreateSnowflakeTable(connectionString, "create or replace table orders (id varchar(50), pic binary);");
                using (var session = store.OpenSession())
                {

                    for (int i = 0; i < 6; i++)
                    {
                        var order = new Orders.Order();
                        session.Store(order);

                        var r = new Random(i);

                        var bytes = new byte[1024 * 1024 * 1];

                        r.NextBytes(bytes);

                        session.Advanced.Attachments.Store(order, "my-attachment", new MemoryStream(bytes));
                    }

                    session.SaveChanges();
                }

                var etlDone = Etl.WaitForEtlToComplete(store, (n, statistics) => statistics.LoadSuccesses >= 5);

                SetupSnowflakeEtl(store, connectionString, @"

var orderData = {
    Id: id(this),
    Pic: loadAttachment('my-attachment') 
};

loadToOrders(orderData);
");
    
                etlDone.Wait(TimeSpan.FromMinutes(5));

                var database = GetDatabase(store.Database).Result;

                var etlProcess = (SnowflakeEtl)database.EtlLoader.Processes.First();

                var stats = etlProcess.GetPerformanceStats();

                Assert.Contains("Stopping the batch because maximum batch size limit was reached (5 MBytes)", stats.Select(x => x.BatchTransformationCompleteReason).ToList());

                etlDone = Etl.WaitForEtlToComplete(store, (n, s) => s.LoadSuccesses >= 6);

                Assert.True(etlDone.Wait(TimeSpan.FromMinutes(1)));
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


    [RequiresSnowflakeFact]
    public async Task CanDelete()
    {
        using (var store = GetDocumentStore(Options.ForMode(RavenDatabaseMode.Single)))
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
                            new OrderLine { Cost = 3, Product = "Milk", Quantity = 3 }, new OrderLine { Cost = 4, Product = "Bear", Quantity = 2 },
                        }
                    });
                    await session.SaveChangesAsync();
                }

                var etlDone = Etl.WaitForEtlToComplete(store);

                SetupSnowflakeEtl(store, connectionString, DefaultScript);

                etlDone.Wait(TimeSpan.FromMinutes(5));

                AssertCounts(1, 2, connectionString);

                etlDone.Reset();

                using (var commands = store.Commands())
                    await commands.DeleteAsync("orders/1-A", null);
                
                etlDone.Wait(TimeSpan.FromMinutes(5));

                AssertCounts(0, 0, connectionString);
            }
        }
    }
    
    [RequiresSnowflakeFact]
    public async Task ShouldImportTask()
    {
        using (var srcStore = GetDocumentStore())
        using (var dstStore = GetDocumentStore())
        using (WithSnowflakeDatabase(out var connectionString, out var _, out var _))
        {
            SetupSnowflakeEtl(srcStore, connectionString, DefaultScript);
            var exportFile = GetTempFileName();

            var exportOperation = await srcStore.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), exportFile);
            await exportOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

            var operation = await dstStore.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), exportFile);
            await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

            var destinationRecord =
                await dstStore.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(dstStore.Database));
            Assert.Equal(1, destinationRecord.SnowflakeEtls.Count);
            Assert.Equal(1, destinationRecord.SnowflakeConnectionStrings.Count);

            Assert.Equal(DefaultScript, destinationRecord.SnowflakeEtls[0].Transforms[0].Script);
            Assert.Equal(["Orders"], destinationRecord.SnowflakeEtls[0].Transforms[0].Collections);
            Assert.Equal("OrdersAndLines", destinationRecord.SnowflakeEtls[0].Transforms[0].Name);

            Assert.Equal(2, destinationRecord.SnowflakeEtls[0].SnowflakeTables.Count);
            Assert.Equal("Orders", destinationRecord.SnowflakeEtls[0].SnowflakeTables[0].TableName);
            Assert.Equal("OrderLines", destinationRecord.SnowflakeEtls[0].SnowflakeTables[1].TableName);
            
            Assert.Equal("Id", destinationRecord.SnowflakeEtls[0].SnowflakeTables[0].DocumentIdColumn);
            Assert.Equal("OrderId", destinationRecord.SnowflakeEtls[0].SnowflakeTables[1].DocumentIdColumn);
            
            Assert.False(destinationRecord.SnowflakeEtls[0].SnowflakeTables[0].InsertOnlyMode);
            Assert.False(destinationRecord.SnowflakeEtls[0].SnowflakeTables[1].InsertOnlyMode);
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
