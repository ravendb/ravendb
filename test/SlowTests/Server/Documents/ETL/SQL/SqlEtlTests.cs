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
using Raven.Server.Documents.ETL.Providers.SQL;
using Raven.Server.Documents.ETL.Providers.SQL.RelationalWriters;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Sparrow;
using Xunit;

namespace SlowTests.Server.Documents.ETL.SQL
{
    public class SqlEtlTests : EtlTestBase
    {
        public static readonly Lazy<string> MasterDatabaseConnection = new Lazy<string>(() =>
        {
            var cString = @"Data Source=localhost\sqlexpress;Integrated Security=SSPI;Connection Timeout=3";

            if (TryConnect(cString))
                return cString;

            cString = @"Data Source=ci1\sqlexpress;Integrated Security=SSPI;Connection Timeout=3";

            if (TryConnect(cString))
                return cString;

            cString = Environment.GetEnvironmentVariable("RAVEN_MSSQL_CONNECTION_STRING");

            if (TryConnect(cString))
                return cString;

            throw new InvalidOperationException("Use a valid connection");

            bool TryConnect(string connectionString)
            {
                if (string.IsNullOrWhiteSpace(connectionString))
                    return false;

                try
                {
                    using (var connection = new SqlConnection(connectionString))
                    {
                        connection.Open();
                    }

                    return true;
                }
                catch (Exception)
                {
                    return false;
                }
            }
        });

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
                CreateRdbmsSchema(store);

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

                var etlDone = WaitForEtl(store, (n, s) => GetOrdersCount(store) == testCount);

                SetupSqlEtl(store, defaultScript);

                etlDone.Wait(TimeSpan.FromMinutes(5));

                Assert.Equal(testCount, GetOrdersCount(store));
            }
        }

        protected void CreateRdbmsSchema(DocumentStore store, string command = @"
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
            CreateRdbmsDatabase(store);

            using (var con = new SqlConnection())
            {
                con.ConnectionString = GetConnectionString(store);
                con.Open();

                using (var dbCommand = con.CreateCommand())
                {
                    dbCommand.CommandText = command;
                    dbCommand.ExecuteNonQuery();
                }
            }
        }

        public override void Dispose()
        {
            base.Dispose();

            using (var con = new SqlConnection())
            {
                con.ConnectionString = MasterDatabaseConnection.Value;
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

        private void CreateRdbmsDatabase(DocumentStore store)
        {
            using (var con = new SqlConnection())
            {
                con.ConnectionString = MasterDatabaseConnection.Value;
                con.Open();

                using (var dbCommand = con.CreateCommand())
                {
                    _dbNames.Add(store.Database);
                    dbCommand.CommandText = $@"
USE master
IF EXISTS(select * from sys.databases where name='SqlReplication-{store.Database}')
DROP DATABASE [SqlReplication-{store.Database}]

CREATE DATABASE [SqlReplication-{store.Database}]
";
                    dbCommand.ExecuteNonQuery();
                }
            }
        }

        [Fact]
        public async Task SimpleTransformation()
        {
            using (var store = GetDocumentStore())
            {
                CreateRdbmsSchema(store);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order
                    {
                        OrderLines = new List<OrderLine>
                        {
                            new OrderLine{Cost = 3, Product = "Milk", Quantity = 3},
                            new OrderLine{Cost = 4, Product = "Bear", Quantity = 2},
                        }
                    });
                    await session.SaveChangesAsync();
                }

                var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

                SetupSqlEtl(store, defaultScript);

                etlDone.Wait(TimeSpan.FromMinutes(5));

                using (var con = new SqlConnection())
                {
                    con.ConnectionString = GetConnectionString(store);
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

        [Fact]
        public async Task NullPropagation()
        {
            using (var store = GetDocumentStore())
            {
                CreateRdbmsSchema(store);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order
                    {
                        OrderLines = new List<OrderLine>
                        {
                            new OrderLine{Cost = 3, Product = "Milk", Quantity = 3},
                            new OrderLine{Cost = 4, Product = "Bear", Quantity = 2},
                        }
                    });
                    await session.SaveChangesAsync();
                }
                var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

                SetupSqlEtl(store, @"var orderData = {
    Id: id(this),
    OrderLinesCount: this.OrderLines_Missing.length,
    TotalCost: 0
};
loadToOrders(orderData);");

                etlDone.Wait(TimeSpan.FromMinutes(5));

                using (var con = new SqlConnection())
                {
                    con.ConnectionString = GetConnectionString(store);
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

        [Fact]
        public async Task NullPropagation_WithExplicitNull()
        {
            using (var store = GetDocumentStore())
            {
                CreateRdbmsSchema(store);


                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order
                    {
                        Address = null,
                        OrderLines = new List<OrderLine>
                        {
                            new OrderLine{Cost = 3, Product = "Milk", Quantity = 3},
                            new OrderLine{Cost = 4, Product = "Bear", Quantity = 2},
                        }
                    });
                    await session.SaveChangesAsync();
                }

                var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

                SetupSqlEtl(store, @"var orderData = {
    Id: id(this),
    City: this.Address.City,
    TotalCost: 0
};
loadToOrders(orderData);");

                etlDone.Wait(TimeSpan.FromMinutes(5));

                using (var con = new SqlConnection())
                {
                    con.ConnectionString = GetConnectionString(store);
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

        [Fact]
        public async Task RavenDB_3341()
        {
            using (var store = GetDocumentStore())
            {
                CreateRdbmsSchema(store);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order
                    {
                        OrderLines = new List<OrderLine>
                        {
                            new OrderLine {Cost = 4, Product = "Bear", Quantity = 2},
                        }
                    });
                    await session.SaveChangesAsync();
                }

                var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

                SetupSqlEtl(store, "if(this.OrderLines.length > 0) { \r\n" + defaultScript + " \r\n}");

                etlDone.Wait(TimeSpan.FromMinutes(5));

                AssertCounts(1, 1, store);

                etlDone.Reset();
                using (var session = store.OpenAsyncSession())
                {
                    var order = await session.LoadAsync<Order>("orders/1-A");
                    order.OrderLines.Clear();
                    await session.SaveChangesAsync();
                }
                etlDone.Wait(TimeSpan.FromMinutes(5));
                AssertCounts(0, 0, store);
            }
        }

        protected static int GetOrdersCount(DocumentStore store)
        {
            using (var con = new SqlConnection())
            {
                con.ConnectionString = GetConnectionString(store);
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
                CreateRdbmsSchema(store);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order
                    {
                        OrderLines = new List<OrderLine>
                        {
                            new OrderLine{Cost = 3, Product = "Milk", Quantity = 3},
                            new OrderLine{Cost = 4, Product = "Bear", Quantity = 2},
                        }
                    });
                    await session.SaveChangesAsync();
                }

                var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

                SetupSqlEtl(store, defaultScript);

                etlDone.Wait(TimeSpan.FromMinutes(5));

                AssertCounts(1, 2, store);

                etlDone.Reset();

                using (var session = store.OpenAsyncSession())
                {
                    var order = await session.LoadAsync<Order>("orders/1-A");
                    order.OrderLines.Clear();
                    await session.SaveChangesAsync();
                }

                etlDone.Wait(TimeSpan.FromMinutes(5));
                AssertCounts(1, 0, store);
            }
        }

        [Fact]
        public async Task CanDelete()
        {
            using (var store = GetDocumentStore())
            {
                CreateRdbmsSchema(store);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order
                    {
                        OrderLines = new List<OrderLine>
                        {
                            new OrderLine{Cost = 3, Product = "Milk", Quantity = 3},
                            new OrderLine{Cost = 4, Product = "Bear", Quantity = 2},
                        }
                    });
                    await session.SaveChangesAsync();
                }

                var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

                SetupSqlEtl(store, defaultScript);

                etlDone.Wait(TimeSpan.FromMinutes(5));

                AssertCounts(1, 2, store);

                etlDone.Reset();

                using (var commands = store.Commands())
                    await commands.DeleteAsync("orders/1-A", null);

                etlDone.Wait(TimeSpan.FromMinutes(5));

                AssertCounts(0, 0, store);
            }
        }

        [Fact]
        public async Task RavenDB_3172()
        {
            using (var store = GetDocumentStore())
            {
                CreateRdbmsSchema(store);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order
                    {
                        Id = "orders/1",
                        OrderLines = new List<OrderLine>
                        {
                            new OrderLine{Cost = 3, Product = "Milk", Quantity = 3},
                            new OrderLine{Cost = 4, Product = "Bear", Quantity = 2},
                        }
                    });
                    await session.SaveChangesAsync();
                }

                var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

                SetupSqlEtl(store, defaultScript, insertOnly: true);

                etlDone.Wait(TimeSpan.FromMinutes(5));

                AssertCounts(1, 2, store);

                etlDone.Reset();

                using (var session = store.OpenAsyncSession())
                {
                    var order = await session.LoadAsync<Order>("orders/1");
                    order.OrderLines.Add(new OrderLine
                    {
                        Cost = 5,
                        Product = "Sugar",
                        Quantity = 7
                    });
                    await session.SaveChangesAsync();
                }

                etlDone.Wait(TimeSpan.FromMinutes(5));
                // we end up with duplicates
                AssertCounts(2, 5, store);
            }
        }

        [Fact]
        public async Task WillLog()
        {
            using (var client = new ClientWebSocket())
            using (var store = GetDocumentStore())
            {
                CreateRdbmsSchema(store);

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
                SetupSqlEtl(store, @"output ('Tralala'); 

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

        [Fact]
        public async Task SimulationTest()
        {
            using (var store = GetDocumentStore())
            {
                CreateRdbmsSchema(store);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order
                    {
                        OrderLines = new List<OrderLine>
                        {
                            new OrderLine{Cost = 3, Product = "Milk", Quantity = 3},
                            new OrderLine{Cost = 4, Product = "Bear", Quantity = 2},
                        }
                    });
                    await session.SaveChangesAsync();
                }

                store.Maintenance.Send(new PutConnectionStringOperation<SqlConnectionString>(new SqlConnectionString()
                {
                    Name = "simulate",
                    ConnectionString = GetConnectionString(store),
                }));

                var database = GetDatabase(store.Database).Result;

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    for (int i = 0; i < 2; i++)
                    {
                        var result = SqlEtl.SimulateSqlEtl(new SimulateSqlEtl
                        {
                            PerformRolledBackTransaction = i % 2 != 0,
                            DocumentId = "orders/1-A",
                            Configuration = new SqlEtlConfiguration()
                            {
                                Name = "simulate",
                                ConnectionStringName = "simulate",
                                FactoryName = "System.Data.SqlClient",
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
                                        Collections = {"Orders"},
                                        Name = "OrdersAndLines",
                                        Script = defaultScript
                                    }
                                }
                            }
                        }, database, database.ServerStore, context);

                        Assert.Null(result.LastAlert);
                        Assert.Equal(2, result.Summary.Count);

                        var orderLines = result.Summary.First(x => x.TableName == "OrderLines");

                        Assert.Equal(3, orderLines.Commands.Length); // delete and two inserts

                        var orders = result.Summary.First(x => x.TableName == "Orders");

                        Assert.Equal(2, orders.Commands.Length); // delete and insert
                    }

                }
            }
        }

        [Fact]
        public async Task LoadingSingleAttachment()
        {
            using (var store = GetDocumentStore())
            {
                CreateRdbmsSchema(store, @"
CREATE TABLE [dbo].[Orders]
(
    [Id] [nvarchar](50) NOT NULL,
    [Name] [nvarchar](255) NULL,
    [Pic] [varbinary](max) NULL
)
");

                var attachmentBytes = new byte[] { 1, 2, 3 };

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order());
                    await session.SaveChangesAsync();
                }

                store.Operations.Send(new PutAttachmentOperation("orders/1-A", "test-attachment", new MemoryStream(attachmentBytes), "image/png"));

                var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses > 0);

                SetupSqlEtl(store, @"
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
                    con.ConnectionString = GetConnectionString(store);
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

        [Fact]
        public async Task LoadingMultipleAttachments()
        {
            using (var store = GetDocumentStore())
            {
                CreateRdbmsSchema(store, @"
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
                    FactoryName = "System.Data.SqlClient",
                    ConnectionStringName = "test",
                    SqlTables =
                    {
                        new SqlEtlTable {TableName = "Attachments", DocumentIdColumn = "UserId", InsertOnlyMode = false},
                    },
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
                }, new SqlConnectionString
                {
                    Name = "test",
                    ConnectionString = GetConnectionString(store)
                });

                etlDone.Wait(TimeSpan.FromMinutes(5));

                using (var con = new SqlConnection())
                {
                    con.ConnectionString = GetConnectionString(store);
                    con.Open();

                    using (var dbCommand = con.CreateCommand())
                    {
                        dbCommand.CommandText = " SELECT COUNT(*) FROM Attachments";
                        Assert.Equal(2, dbCommand.ExecuteScalar());
                    }
                }
            }
        }

        [Fact]
        public async Task LoadingNonExistingAttachmentWillStoreNull()
        {
            using (var store = GetDocumentStore())
            {
                CreateRdbmsSchema(store, @"
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

                SetupSqlEtl(store, @"
var orderData = {
    Id: id(this),
    Pic: loadAttachment('non-existing')
};

loadToOrders(orderData);
");

                etlDone.Wait(TimeSpan.FromMinutes(5));

                using (var con = new SqlConnection())
                {
                    con.ConnectionString = GetConnectionString(store);
                    con.Open();

                    using (var dbCommand = con.CreateCommand())
                    {
                        dbCommand.CommandText = " SELECT Pic FROM Orders WHERE Id = 'orders/1-A'";

                        var sqlDataReader = dbCommand.ExecuteReader();


                        Assert.True(sqlDataReader.Read());
                        Assert.True(sqlDataReader.IsDBNull(0));
                    }
                }
            }
        }

        [Fact]
        public async Task LoadingFromMultipleCollections()
        {
            using (var store = GetDocumentStore())
            {
                CreateRdbmsSchema(store);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order
                    {
                        OrderLines = new List<OrderLine>
                        {
                            new OrderLine{Cost = 3, Product = "Milk", Quantity = 3},
                            new OrderLine{Cost = 4, Product = "Bear", Quantity = 2},
                        }
                    });

                    await session.StoreAsync(new FavouriteOrder
                    {
                        OrderLines = new List<OrderLine>
                        {
                            new OrderLine{Cost = 3, Product = "Milk", Quantity = 3},
                        }
                    });

                    await session.SaveChangesAsync();
                }

                var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

                SetupSqlEtl(store, defaultScript, collections: new List<string> { "Orders", "FavouriteOrders" });

                etlDone.Wait(TimeSpan.FromMinutes(5));

                using (var con = new SqlConnection())
                {
                    con.ConnectionString = GetConnectionString(store);
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

        [Fact]
        public async Task CanUseVarcharAndNVarcharFunctions()
        {
            using (var store = GetDocumentStore())
            {
                CreateRdbmsSchema(store, @"
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
                    await session.StoreAsync(new User
                    {
                        Name = "Joe Doń"
                    });

                    await session.SaveChangesAsync();
                }

                var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses > 0);

                AddEtl(store, new SqlEtlConfiguration()
                {
                    Name = "CanUserNonVarcharAndNVarcharFunctions",
                    FactoryName = "System.Data.SqlClient",
                    ConnectionStringName = "test",
                    SqlTables =
                    {
                        new SqlEtlTable {TableName = "Users", DocumentIdColumn = "Id", InsertOnlyMode = false},
                    },
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
                }, new SqlConnectionString
                {
                    Name = "test",
                    ConnectionString = GetConnectionString(store)
                });

                etlDone.Wait(TimeSpan.FromMinutes(5));

                using (var con = new SqlConnection())
                {
                    con.ConnectionString = GetConnectionString(store);
                    con.Open();

                    using (var dbCommand = con.CreateCommand())
                    {
                        dbCommand.CommandText = " SELECT COUNT(*) FROM Users";
                        Assert.Equal(1, dbCommand.ExecuteScalar());
                    }
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

        private static void AssertCounts(int ordersCount, int orderLineCounts, DocumentStore store)
        {
            using (var con = new SqlConnection())
            {
                con.ConnectionString = GetConnectionString(store);
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

        protected static void SetupSqlEtl(DocumentStore store, string script, bool insertOnly = false, List<string> collections = null)
        {
            var connectionStringName = $"{store.Database}@{store.Urls.First()} to SQL DB";

            AddEtl(store, new SqlEtlConfiguration()
            {
                Name = connectionStringName,
                ConnectionStringName = connectionStringName,
                FactoryName = "System.Data.SqlClient",
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
                ConnectionString = GetConnectionString(store)
            });
        }

        public static string GetConnectionString(DocumentStore store)
        {
            return MasterDatabaseConnection.Value + $";Initial Catalog=SqlReplication-{store.Database};";
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
