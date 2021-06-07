// -----------------------------------------------------------------------
//  <copyright file="SqlEtlTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using FastTests.Voron.Util;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Extensions;
using Raven.Server.Config;
using Raven.Server.Documents.ETL.Providers.SQL;
using Raven.Server.Documents.ETL.Providers.SQL.RelationalWriters;
using Raven.Server.Documents.ETL.Providers.SQL.Test;
using Raven.Server.ServerWide.Context;
using Raven.Server.SqlMigration;
using Raven.Tests.Core.Utils.Entities;
using SlowTests.Server.Documents.Migration;
using Sparrow.Server;
using Tests.Infrastructure.ConnectionString;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL.SQL
{
    public class SqlEtlTests : EtlTestBase
    {
        public SqlEtlTests(ITestOutputHelper output) : base(output)
        {
        }

        private readonly List<string> _dbNames = new List<string>();

        protected const string defaultScript = @"
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

        [Fact]
        public async Task ReplicateMultipleBatches()
        {
            using (var store = GetDocumentStore())
            {
                using (SqlAwareTestBase.WithSqlDatabase(MigrationProvider.MsSQL, out var connectionString, out string schemaName, dataSet: null, includeData: false))
                {
                    CreateRdbmsSchema(connectionString);
                    int testCount = 5000;

                    using (var bulkInsert = store.BulkInsert())
                    {
                        for (int i = 0; i < testCount; i++)
                        {
                            await bulkInsert.StoreAsync(new Order
                            {
                                OrderLines = new List<OrderLine>
                            {
                                new OrderLine {Cost = 3, Product = "Milk", Quantity = 3},
                                new OrderLine {Cost = 4, Product = "Bear", Quantity = 2},
                            }
                            });
                        }
                    }

                    var etlDone = WaitForEtl(store, (n, s) => GetOrdersCount(connectionString) == testCount);

                    SetupSqlEtl(store, connectionString, defaultScript);

                    etlDone.Wait(TimeSpan.FromMinutes(5));

                    Assert.Equal(testCount, GetOrdersCount(connectionString));
                }
            }
        }

        protected void CreateRdbmsSchema(string connectionString, string command = @"
CREATE TABLE [dbo].[OrderLines]
(
    [Id] int identity primary key,
    [OrderId] [nvarchar](50) NOT NULL,
    [Qty] [int] NOT NULL,
    [Product] [nvarchar](255) NOT NULL,
    [Cost] [int] NOT NULL
)

CREATE TABLE [dbo].[Orders]
(
    [Id] [nvarchar](50) NOT NULL,
    [OrderLinesCount] [int]  NULL,
    [TotalCost] [int] NOT NULL,
    [City] [nvarchar](50) NULL
)
")
        {
            using (var con = new SqlConnection())
            {
                con.ConnectionString = connectionString;
                con.Open();

                using (var dbCommand = con.CreateCommand())
                {
                    dbCommand.CommandText = command;
                    dbCommand.ExecuteNonQuery();
                }
                con.Close();
            }
        }

        public override void Dispose()
        {
            base.Dispose();

            using (var con = new SqlConnection())
            {
                con.ConnectionString = MssqlConnectionString.Instance.VerifiedConnectionString.Value;
                con.Open();

                foreach (var dbName in _dbNames)
                {
                    using (var dbCommand = con.CreateCommand())
                    {
                        dbCommand.CommandText = $@"
ALTER DATABASE [SqlReplication-{dbName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
DROP DATABASE [SqlReplication-{dbName}]";
                        dbCommand.ExecuteNonQuery();
                    }
                }
            }
        }

        [Fact]
        public async Task SimpleTransformation()
        {
            using (var store = GetDocumentStore())
            {
                using (SqlAwareTestBase.WithSqlDatabase(MigrationProvider.MsSQL, out var connectionString, out string schemaName, dataSet: null, includeData: false))
                {
                    CreateRdbmsSchema(connectionString);

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

                    var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

                    SetupSqlEtl(store, connectionString, defaultScript);

                    etlDone.Wait(TimeSpan.FromMinutes(5));

                    using (var con = new SqlConnection())
                    {
                        con.ConnectionString = connectionString;
                        con.Open();

                        using (var dbCommand = con.CreateCommand())
                        {
                            dbCommand.CommandText = " SELECT COUNT(*) FROM Orders";
                            Assert.Equal(1, dbCommand.ExecuteScalar());
                            dbCommand.CommandText = " SELECT COUNT(*) FROM OrderLines";
                            Assert.Equal(2, dbCommand.ExecuteScalar());
                        }
                    }
                }
            }
        }

        [Fact]
        public async Task NullPropagation()
        {
            using (var store = GetDocumentStore())
            {
                using (SqlAwareTestBase.WithSqlDatabase(MigrationProvider.MsSQL, out var connectionString, out string schemaName, dataSet: null, includeData: false))
                {
                    CreateRdbmsSchema(connectionString);

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

                    var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

                    SetupSqlEtl(store, connectionString, @"var orderData = {
    Id: id(this),
    OrderLinesCount: this.OrderLines_Missing.length,
    TotalCost: 0
};
loadToOrders(orderData);");

                    etlDone.Wait(TimeSpan.FromMinutes(5));

                    using (var con = new SqlConnection())
                    {
                        con.ConnectionString = connectionString;
                        con.Open();

                        using (var dbCommand = con.CreateCommand())
                        {
                            dbCommand.CommandText = " SELECT COUNT(*) FROM Orders";
                            Assert.Equal(1, dbCommand.ExecuteScalar());
                            dbCommand.CommandText = " SELECT OrderLinesCount FROM Orders";
                            Assert.Equal(0, dbCommand.ExecuteScalar());
                        }
                    }
                }
            }
        }

        [Fact]
        public async Task NullPropagation_WithExplicitNull()
        {
            using (var store = GetDocumentStore())
            {
                using (SqlAwareTestBase.WithSqlDatabase(MigrationProvider.MsSQL, out var connectionString, out string schemaName, dataSet: null, includeData: false))
                {
                    CreateRdbmsSchema(connectionString);

                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new Order
                        {
                            Address = null,
                            OrderLines = new List<OrderLine>
                            {
                                new OrderLine {Cost = 3, Product = "Milk", Quantity = 3}, new OrderLine {Cost = 4, Product = "Bear", Quantity = 2},
                            }
                        });
                        await session.SaveChangesAsync();
                    }

                    var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

                    SetupSqlEtl(store, connectionString, @"var orderData = {
    Id: id(this),
    City: this.Address.City,
    TotalCost: 0
};
loadToOrders(orderData);");

                    etlDone.Wait(TimeSpan.FromMinutes(5));

                    using (var con = new SqlConnection())
                    {
                        con.ConnectionString = connectionString;
                        con.Open();

                        using (var dbCommand = con.CreateCommand())
                        {
                            dbCommand.CommandText = " SELECT COUNT(*) FROM Orders";
                            Assert.Equal(1, dbCommand.ExecuteScalar());
                            dbCommand.CommandText = " SELECT City FROM Orders";
                            Assert.Equal(DBNull.Value, dbCommand.ExecuteScalar());
                        }
                    }
                }
            }
        }

        [Fact]
        public async Task RavenDB_3341()
        {
            using (var store = GetDocumentStore())
            {
                using (SqlAwareTestBase.WithSqlDatabase(MigrationProvider.MsSQL, out var connectionString, out string schemaName, dataSet: null, includeData: false))
                {
                    CreateRdbmsSchema(connectionString);

                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new Order { OrderLines = new List<OrderLine> { new OrderLine { Cost = 4, Product = "Bear", Quantity = 2 }, } });
                        await session.SaveChangesAsync();
                    }

                    var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

                    SetupSqlEtl(store, connectionString, "if(this.OrderLines.length > 0) { \r\n" + defaultScript + " \r\n}");

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

        protected static int GetOrdersCount(string connectionString)
        {
            using (var con = new SqlConnection())
            {
                con.ConnectionString = connectionString;
                con.Open();

                using (var dbCommand = con.CreateCommand())
                {
                    dbCommand.CommandText = " SELECT COUNT(*) FROM Orders";
                    return (int)dbCommand.ExecuteScalar();
                }
            }
        }

        [Fact]
        public async Task CanUpdateToBeNoItemsInChildTable()
        {
            using (var store = GetDocumentStore())
            {
                using (SqlAwareTestBase.WithSqlDatabase(MigrationProvider.MsSQL, out var connectionString, out string schemaName, dataSet: null, includeData: false))
                {
                    CreateRdbmsSchema(connectionString);


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

                    var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

                    SetupSqlEtl(store, connectionString, defaultScript);

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

        [Fact]
        public async Task CanDelete()
        {
            using (var store = GetDocumentStore())
            {
                using (SqlAwareTestBase.WithSqlDatabase(MigrationProvider.MsSQL, out var connectionString, out string schemaName, dataSet: null, includeData: false))
                {
                    CreateRdbmsSchema(connectionString);

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

                    var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

                    SetupSqlEtl(store, connectionString, defaultScript);

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

        [Fact]
        public async Task RavenDB_3172()
        {
            using (var store = GetDocumentStore())
            {
                using (SqlAwareTestBase.WithSqlDatabase(MigrationProvider.MsSQL, out var connectionString, out string schemaName, dataSet: null, includeData: false))
                {
                    CreateRdbmsSchema(connectionString);
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

                    var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

                    SetupSqlEtl(store, connectionString, defaultScript, insertOnly: true);

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

        [Fact]
        public async Task WillLog()
        {
            using (var client = new ClientWebSocket())
            using (var store = GetDocumentStore())
            {
                using (SqlAwareTestBase.WithSqlDatabase(MigrationProvider.MsSQL, out var connectionString, out string schemaName, dataSet: null, includeData: false))
                {
                    CreateRdbmsSchema(connectionString);
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
                    SetupSqlEtl(store, connectionString, @"output ('Tralala'); 

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
                        var msg = "Could not process SQL Replication script for OrdersAndLines, skipping document: orders/";
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

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task CanTestScript(bool performRolledBackTransaction)
        {
            using (var store = GetDocumentStore())
            {
                using (SqlAwareTestBase.WithSqlDatabase(MigrationProvider.MsSQL, out var connectionString, out string schemaName, dataSet: null, includeData: false))
                {
                    CreateRdbmsSchema(connectionString);
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

                    var result1 = store.Maintenance.Send(new PutConnectionStringOperation<SqlConnectionString>(new SqlConnectionString()
                    {
                        Name = "simulate", ConnectionString = connectionString, FactoryName = "System.Data.SqlClient",
                    }));
                    Assert.NotNull(result1.RaftCommandIndex);

                    var database = GetDatabase(store.Database).Result;

                    using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    {
                        var result = (SqlEtlTestScriptResult)SqlEtl.TestScript(
                            new TestSqlEtlScript
                            {
                                PerformRolledBackTransaction = performRolledBackTransaction,
                                DocumentId = "orders/1-A",
                                Configuration = new SqlEtlConfiguration()
                                {
                                    Name = "simulate",
                                    ConnectionStringName = "simulate",
                                    SqlTables =
                                    {
                                        new SqlEtlTable {TableName = "Orders", DocumentIdColumn = "Id"},
                                        new SqlEtlTable {TableName = "OrderLines", DocumentIdColumn = "OrderId"},
                                        new SqlEtlTable {TableName = "NotUsedInScript", DocumentIdColumn = "OrderId"},
                                    },
                                    Transforms =
                                    {
                                        new Transformation()
                                        {
                                            Collections = {"Orders"}, Name = "OrdersAndLines", Script = defaultScript + "output('test output')"
                                        }
                                    }
                                }
                            }, database, database.ServerStore, context);

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

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task CanTestDeletion(bool performRolledBackTransaction)
        {
            using (var store = GetDocumentStore())
            {
                using (SqlAwareTestBase.WithSqlDatabase(MigrationProvider.MsSQL, out var connectionString, out string schemaName, dataSet: null, includeData: false))
                {
                    CreateRdbmsSchema(connectionString);

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

                    var result1 = store.Maintenance.Send(new PutConnectionStringOperation<SqlConnectionString>(new SqlConnectionString()
                    {
                        Name = "simulate",
                        ConnectionString = connectionString,
                        FactoryName = "System.Data.SqlClient",
                    }));
                    Assert.NotNull(result1.RaftCommandIndex);

                    var database = GetDatabase(store.Database).Result;

                    using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    {
                        var result = (SqlEtlTestScriptResult)SqlEtl.TestScript(
                            new TestSqlEtlScript
                            {
                                PerformRolledBackTransaction = performRolledBackTransaction,
                                DocumentId = "orders/1-A",
                                IsDelete = true,
                                Configuration = new SqlEtlConfiguration()
                                {
                                    Name = "simulate",
                                    ConnectionStringName = "simulate",
                                    SqlTables =
                                    {
                                        new SqlEtlTable {TableName = "Orders", DocumentIdColumn = "Id"},
                                        new SqlEtlTable {TableName = "OrderLines", DocumentIdColumn = "OrderId"},
                                        new SqlEtlTable {TableName = "NotUsedInScript", DocumentIdColumn = "OrderId"},
                                    },
                                    Transforms = { new Transformation() { Collections = { "Orders" }, Name = "OrdersAndLines", Script = defaultScript } }
                                }
                            }, database, database.ServerStore, context);

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
                        Assert.NotNull(session.Query<Order>("orders/1-A"));
                    }
                }
            }
        }

        [Fact]
        public async Task LoadingSingleAttachment()
        {
            using (var store = GetDocumentStore())
            {
                using (SqlAwareTestBase.WithSqlDatabase(MigrationProvider.MsSQL, out var connectionString, out string schemaName, dataSet: null, includeData: false))
                {
                    CreateRdbmsSchema(connectionString, @"
CREATE TABLE [dbo].[Orders]
(
    [Id] [nvarchar](50) NOT NULL,
    [Name] [nvarchar](255) NULL,
    [Pic] [varbinary](max) NULL
)
");

                    var attachmentBytes = new byte[] {1, 2, 3};

                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new Order());
                        await session.SaveChangesAsync();
                    }

                    store.Operations.Send(new PutAttachmentOperation("orders/1-A", "test-attachment", new MemoryStream(attachmentBytes), "image/png"));

                    var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses > 0);

                    SetupSqlEtl(store, connectionString, @"
var orderData = {
    Id: id(this),
    Name: this['@metadata']['@attachments'][0].Name,
    Pic: loadAttachment(this['@metadata']['@attachments'][0].Name)
};

loadToOrders(orderData);
");

                    etlDone.Wait(TimeSpan.FromMinutes(5));

                    using (var con = new SqlConnection())
                    {
                        con.ConnectionString = connectionString;
                        con.Open();

                        using (var dbCommand = con.CreateCommand())
                        {
                            dbCommand.CommandText = " SELECT COUNT(*) FROM Orders";
                            Assert.Equal(1, dbCommand.ExecuteScalar());
                        }

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

        [Fact]
        public async Task Should_error_if_attachment_doesnt_exist()
        {
            using (var store = GetDocumentStore())
            {
                using (SqlAwareTestBase.WithSqlDatabase(MigrationProvider.MsSQL, out var connectionString, out string schemaName, dataSet: null, includeData: false))
                {
                    CreateRdbmsSchema(connectionString, @"
CREATE TABLE [dbo].[Orders]
(
    [Id] [nvarchar](50) NOT NULL,
    [Name] [nvarchar](255) NULL,
    [Pic] [varbinary](max) NULL
)
");

                    var attachmentBytes = new byte[] {1, 2, 3};

                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new Order(), "orders/1-A");
                        await session.StoreAsync(new Order(), "orders/2-A");
                        await session.StoreAsync(new Order(), "orders/3-A");

                        await session.SaveChangesAsync();
                    }

                    store.Operations.Send(new PutAttachmentOperation("orders/1-A", "abc.jpg", new MemoryStream(attachmentBytes), "image/png"));
                    store.Operations.Send(new PutAttachmentOperation("orders/2-A", "photo.jpg", new MemoryStream(attachmentBytes), "image/png"));

                    var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses > 0);

                    SetupSqlEtl(store, connectionString, @"
var orderData = {
    Id: id(this),
    Name: 'photo.jpg',
    Pic: loadAttachment('photo.jpg')
};

loadToOrders(orderData);
");

                    etlDone.Wait(TimeSpan.FromMinutes(5));

                    using (var con = new SqlConnection())
                    {
                        con.ConnectionString = connectionString;
                        con.Open();

                        using (var dbCommand = con.CreateCommand())
                        {
                            dbCommand.CommandText = " SELECT COUNT(*) FROM Orders";
                            Assert.Equal(1, dbCommand.ExecuteScalar());
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

        [Fact]
        public async Task LoadingMultipleAttachments()
        {
            using (var store = GetDocumentStore())
            {
                using (SqlAwareTestBase.WithSqlDatabase(MigrationProvider.MsSQL, out var connectionString, out string schemaName, dataSet: null, includeData: false))
                {
                    CreateRdbmsSchema(connectionString, @"
CREATE TABLE [dbo].[Attachments]
(
    [Id] int identity primary key,
    [UserId] [nvarchar](50) NOT NULL,
    [AttachmentName] [nvarchar](50) NULL,
    [Data] [varbinary](max) NULL
)
");

                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User());
                        await session.SaveChangesAsync();
                    }

                    store.Operations.Send(new PutAttachmentOperation("users/1-A", "profile.jpg", new MemoryStream(new byte[] { 1, 2, 3, 4, 5, 6, 7 }), "image/jpeg"));
                    store.Operations.Send(new PutAttachmentOperation("users/1-A", "profile-small.jpg", new MemoryStream(new byte[] { 1, 2, 3 }), "image/jpeg"));

                    var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses > 0);

                    AddEtl(store, new SqlEtlConfiguration()
                    {
                        Name = "LoadingMultipleAttachments",
                        ConnectionStringName = "test",
                        SqlTables = { new SqlEtlTable { TableName = "Attachments", DocumentIdColumn = "UserId", InsertOnlyMode = false }, },
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
                    }, new SqlConnectionString { Name = "test", FactoryName = "System.Data.SqlClient", ConnectionString = connectionString });

                    etlDone.Wait(TimeSpan.FromMinutes(5));

                    using (var con = new SqlConnection())
                    {
                        con.ConnectionString = connectionString;
                        con.Open();

                        using (var dbCommand = con.CreateCommand())
                        {
                            dbCommand.CommandText = " SELECT COUNT(*) FROM Attachments";
                            Assert.Equal(2, dbCommand.ExecuteScalar());
                        }
                    }
                }
            }
        }

        [Fact]
        public async Task CanSkipSettingFieldIfAttachmentDoesntExist()
        {
            using (var store = GetDocumentStore())
            {
                using (SqlAwareTestBase.WithSqlDatabase(MigrationProvider.MsSQL, out var connectionString, out string schemaName, dataSet: null, includeData: false))
                {
                    CreateRdbmsSchema(connectionString, @"
CREATE TABLE [dbo].[Orders]
(
    [Id] [nvarchar](50) NOT NULL,
    [Pic] [varbinary](max) NULL
)
");

                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new Order());
                        await session.SaveChangesAsync();
                    }

                    var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses > 0);

                    SetupSqlEtl(store, connectionString, @"

var orderData = {
    Id: id(this),
    // Pic: loadAttachment('non-existing') // skip loading non existing attachment
};

loadToOrders(orderData);
");

                    etlDone.Wait(TimeSpan.FromMinutes(5));

                    using (var con = new SqlConnection())
                    {
                        con.ConnectionString = connectionString;
                        con.Open();

                        using (var dbCommand = con.CreateCommand())
                        {
                            dbCommand.CommandText = " SELECT COUNT(*) FROM Orders";
                            Assert.Equal(1, dbCommand.ExecuteScalar());

                            dbCommand.CommandText = " SELECT Pic FROM Orders WHERE Id = 'orders/1-A'";

                            var sqlDataReader = dbCommand.ExecuteReader();

                            Assert.True(sqlDataReader.Read());
                            Assert.True(sqlDataReader.IsDBNull(0));
                        }
                    }
                }
            }
        }

        [Fact]
        public async Task LoadingFromMultipleCollections()
        {
            using (var store = GetDocumentStore())
            {
                using (SqlAwareTestBase.WithSqlDatabase(MigrationProvider.MsSQL, out var connectionString, out string schemaName, dataSet: null, includeData: false))
                {
                    CreateRdbmsSchema(connectionString);

                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new Order
                        {
                            OrderLines = new List<OrderLine>
                            {
                                new OrderLine {Cost = 3, Product = "Milk", Quantity = 3}, new OrderLine {Cost = 4, Product = "Bear", Quantity = 2},
                            }
                        });

                        await session.StoreAsync(new FavouriteOrder {OrderLines = new List<OrderLine> {new OrderLine {Cost = 3, Product = "Milk", Quantity = 3},}});

                        await session.SaveChangesAsync();
                    }

                    var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

                    SetupSqlEtl(store, connectionString, defaultScript, collections: new List<string> {"Orders", "FavouriteOrders"});

                    etlDone.Wait(TimeSpan.FromMinutes(5));

                    using (var con = new SqlConnection())
                    {
                        con.ConnectionString = connectionString;
                        con.Open();

                        using (var dbCommand = con.CreateCommand())
                        {
                            dbCommand.CommandText = " SELECT COUNT(*) FROM Orders";
                            Assert.Equal(2, dbCommand.ExecuteScalar());
                            dbCommand.CommandText = " SELECT COUNT(*) FROM OrderLines";
                            Assert.Equal(3, dbCommand.ExecuteScalar());
                        }
                    }
                }
            }
        }

            [Fact]
            public async Task CanUseVarcharAndNVarcharFunctions()
            {
                using (var store = GetDocumentStore())
                {
                    using (SqlAwareTestBase.WithSqlDatabase(MigrationProvider.MsSQL, out var connectionString, out string schemaName, dataSet: null, includeData: false))
                    {
                        CreateRdbmsSchema(connectionString, @"
CREATE TABLE [dbo].[Users]
(
    [Id] [nvarchar](50) NOT NULL,
    [FirstName] [varchar](30) NOT NULL,
    [LastName] [nvarchar](30) NULL,
    [FirstName2] [varchar](30) NOT NULL,
    [LastName2] [nvarchar](30) NULL
)
");
                        using (var session = store.OpenAsyncSession())
                        {
                            await session.StoreAsync(new User {Name = "Joe Doń"});

                            await session.SaveChangesAsync();
                        }

                        var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses > 0);

                        AddEtl(store, new SqlEtlConfiguration()
                        {
                            Name = "CanUserNonVarcharAndNVarcharFunctions",
                            ConnectionStringName = "test",
                            SqlTables = {new SqlEtlTable {TableName = "Users", DocumentIdColumn = "Id", InsertOnlyMode = false},},
                            Transforms =
                            {
                                new Transformation()
                                {
                                    Name = "varchartest",
                                    Collections = {"Users"},
                                    Script = @"

var names = this.Name.split(' ');

loadToUsers(
{
    FirstName: varchar(names[0], 30),
    LastName: nvarchar(names[1], 30),
    FirstName2: varchar(names[0]),
    LastName2:  nvarchar(names[1]),
});
"
                                }
                            }
                        }, new SqlConnectionString {Name = "test", FactoryName = "System.Data.SqlClient", ConnectionString = connectionString });

                        etlDone.Wait(TimeSpan.FromMinutes(5));

                        using (var con = new SqlConnection())
                        {
                            con.ConnectionString = connectionString;
                            con.Open();

                            using (var dbCommand = con.CreateCommand())
                            {
                                dbCommand.CommandText = " SELECT COUNT(*) FROM Users";
                                Assert.Equal(1, dbCommand.ExecuteScalar());
                            }
                        }
                    }
                }
            }

            [Fact]
                public void Should_stop_batch_if_size_limit_exceeded_RavenDB_12800()
                {
                    using (var store = GetDocumentStore(new Options { ModifyDatabaseRecord = x => x.Settings[RavenConfiguration.GetKey(c => c.Etl.MaxBatchSize)] = "5" }))
                    {
                        using (SqlAwareTestBase.WithSqlDatabase(MigrationProvider.MsSQL, out var connectionString, out string schemaName, dataSet: null, includeData: false))
                        {
                            CreateRdbmsSchema(connectionString, @"
CREATE TABLE [dbo].[Orders]
(
    [Id] [nvarchar](50) NOT NULL,
    [Pic] [varbinary](max) NULL
)
");
                            using (var session = store.OpenSession())
                            {

                                for (int i = 0; i < 10; i++)
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

                            var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses > 0);

                            SetupSqlEtl(store, connectionString, @"

var orderData = {
    Id: id(this),
    Pic: loadAttachment('my-attachment') 
};

loadToOrders(orderData);
");

                            etlDone.Wait(TimeSpan.FromMinutes(5));

                            var database = GetDatabase(store.Database).Result;

                            var etlProcess = (SqlEtl)database.EtlLoader.Processes.First();

                            var stats = etlProcess.GetPerformanceStats();

                            Assert.Contains("Stopping the batch because maximum batch size limit was reached (5 MBytes)", stats.Select(x => x.BatchCompleteReason).ToList());
                        }
                    }
                }

                private async Task<string> ReadFromWebSocket(ArraySegment<byte> buffer, WebSocket source)
        {
            using (var ms = new MemoryStream())
            {
                WebSocketReceiveResult result;
                do
                {
                    try
                    {
                        result = await source.ReceiveAsync(buffer, CancellationToken.None);
                    }
                    catch (Exception)
                    {
                        break;
                    }
                    ms.Write(buffer.Array, buffer.Offset, result.Count);
                }
                while (!result.EndOfMessage);
                ms.Seek(0, SeekOrigin.Begin);

                return new StreamReader(ms, Encoding.UTF8).ReadToEnd();
            }
        }

        private static void AssertCounts(int ordersCount, int orderLineCounts, string connectionString)
        {
            using (var con = new SqlConnection())
            {
                con.ConnectionString = connectionString;
                con.Open();

                using (var dbCommand = con.CreateCommand())
                {
                    dbCommand.CommandText = "SELECT COUNT(*) FROM Orders";
                    Assert.Equal(ordersCount, dbCommand.ExecuteScalar());
                    dbCommand.CommandText = "SELECT COUNT(*) FROM OrderLines";
                    Assert.Equal(orderLineCounts, dbCommand.ExecuteScalar());
                }
            }
        }

        protected void SetupSqlEtl(DocumentStore store, string connectionString, string script, bool insertOnly = false, List<string> collections = null)
        {
            var connectionStringName = $"{store.Database}@{store.Urls.First()} to SQL DB";

            AddEtl(store, new SqlEtlConfiguration()
            {
                Name = connectionStringName,
                ConnectionStringName = connectionStringName,
                SqlTables =
                {
                    new SqlEtlTable {TableName = "Orders", DocumentIdColumn = "Id", InsertOnlyMode = insertOnly},
                    new SqlEtlTable {TableName = "OrderLines", DocumentIdColumn = "OrderId", InsertOnlyMode = insertOnly},
                },
                Transforms =
                {
                    new Transformation()
                    {
                        Name = "OrdersAndLines",
                        Collections = collections ?? new List<string> {"Orders"},
                        Script = script
                    }
                }
            }, new SqlConnectionString
            {
                Name = connectionStringName,
                ConnectionString = connectionString,
                FactoryName = "System.Data.SqlClient"
            });
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
}
