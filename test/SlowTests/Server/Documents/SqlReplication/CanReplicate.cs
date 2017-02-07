// -----------------------------------------------------------------------
//  <copyright file="CanReplicate.cs" company="Hibernating Rhinos LTD">
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
using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Abstractions.Extensions;
using Raven.NewClient.Client.Document;
using Raven.Server.Documents.SqlReplication;
using Sparrow.Platform;
using Xunit;

namespace SlowTests.Server.Documents.SqlReplication
{
    public class CanReplicate : RavenNewTestBase
    {
        private static readonly Lazy<string> _masterDatabaseConnection = new Lazy<string>(() =>
        {
            var local = @"Data Source=localhost\sqlexpress;Integrated Security=SSPI;Connection Timeout=1";
            try
            {
                using (var con = new SqlConnection(local))
                {
                    con.Open();
                }
                return local;
            }
            catch (Exception)
            {
                try
                {
                    local = @"Data Source=ci1\sqlexpress;Integrated Security=SSPI;Connection Timeout=1";
                    using (var con = new SqlConnection(local))
                    {
                        con.Open();
                    }
                    return local;
                }
                catch
                {
                    try
                    {
                        local = @"Data Source=(localdb)\v11.0;Integrated Security=SSPI;Connection Timeout=1";
                        using (var con = new SqlConnection(local))
                        {
                            con.Open();
                        }
                        return local;
                    }
                    catch
                    {
                        try
                        {
                            string path;
                            if (PlatformDetails.RunningOnPosix)
                                path = @"/tmp/sqlReplicationPassword.txt";
                            else
                                path = @"P:\Build\SqlReplicationPassword.txt";

                            var readAllLines = File.ReadAllLines(path);
                            return $@"Data Source=ci1\sqlexpress;User Id={readAllLines[0]};Password={readAllLines[1]};Connection Timeout=1";
                        }
                        catch (Exception e)
                        {
                            throw new InvalidOperationException("Use a valid connection", e);
                        }

                    }
                }
            }
        });

        private readonly List<string> _dbNames = new List<string>();

        protected const string defaultScript = @"
var orderData = {
    Id: documentId,
    OrderLinesCount: this.OrderLines.length,
    TotalCost: 0
};
replicateToOrders(orderData);

for (var i = 0; i < this.OrderLines.length; i++) {
    var line = this.OrderLines[i];
    orderData.TotalCost += line.Cost;
    replicateToOrderLines({
        OrderId: documentId,
        Qty: line.Quantity,
        Product: line.Product,
        Cost: line.Cost
    });
}";

        [NonLinuxFact]
        public async Task ReplicateMultipleBatches()
        {
            using (var store = GetDocumentStore())
            {
                CreateRdbmsSchema(store);

                var eventSlim = new ManualResetEventSlim(false);
                var database = await GetDatabase(store.DefaultDatabase);
                int testCount = 5000;
                database.SqlReplicationLoader.AfterReplicationCompleted += statistics =>
                {
                    if (GetOrdersCount(store) == testCount)
                        eventSlim.Set();
                };

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

                await SetupSqlReplication(store, defaultScript);

                eventSlim.Wait(TimeSpan.FromMinutes(5));

                Assert.Equal(testCount, GetOrdersCount(store));
            }
        }

        protected void CreateRdbmsSchema(DocumentStore store)
        {
            CreateRdbmsDatabase(store);

            using (var con = new SqlConnection())
            {
                con.ConnectionString = GetConnectionString(store);
                con.Open();

                using (var dbCommand = con.CreateCommand())
                {
                    dbCommand.CommandText = @"
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
";
                    dbCommand.ExecuteNonQuery();
                }
            }
        }

        public override void Dispose()
        {
            base.Dispose();

            using (var con = new SqlConnection())
            {
                con.ConnectionString = _masterDatabaseConnection.Value;
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
                con.ConnectionString = _masterDatabaseConnection.Value;
                con.Open();

                using (var dbCommand = con.CreateCommand())
                {
                    _dbNames.Add(store.DefaultDatabase);
                    dbCommand.CommandText = $@"
USE master
IF EXISTS(select * from sys.databases where name='SqlReplication-{store.DefaultDatabase}')
DROP DATABASE [SqlReplication-{store.DefaultDatabase}]

CREATE DATABASE [SqlReplication-{store.DefaultDatabase}]
";
                    dbCommand.ExecuteNonQuery();
                }
            }
        }

        [NonLinuxFact]
        public async Task SimpleTransformation()
        {
            using (var store = GetDocumentStore())
            {
                CreateRdbmsSchema(store);

                var eventSlim = new ManualResetEventSlim(false);
                var database = await GetDatabase(store.DefaultDatabase);
                database.SqlReplicationLoader.AfterReplicationCompleted += statistics =>
                {
                    if (statistics.SuccessCount != 0)
                        eventSlim.Set();
                };

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

                await SetupSqlReplication(store, defaultScript);

                eventSlim.Wait(TimeSpan.FromMinutes(5));

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

        [NonLinuxFact]
        public async Task NullPropagation()
        {
            using (var store = GetDocumentStore())
            {
                CreateRdbmsSchema(store);

                var eventSlim = new ManualResetEventSlim(false);
                var database = await GetDatabase(store.DefaultDatabase);
                database.SqlReplicationLoader.AfterReplicationCompleted += statistics =>
                {
                    if (statistics.SuccessCount != 0)
                        eventSlim.Set();
                };

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

                await SetupSqlReplication(store, @"var orderData = {
    Id: documentId,
    OrderLinesCount: this.OrderLines_Missing.length,
    TotalCost: 0
};
replicateToOrders(orderData);");

                eventSlim.Wait(TimeSpan.FromMinutes(5));

                using (var con = new SqlConnection())
                {
                    con.ConnectionString = GetConnectionString(store);
                    con.Open();

                    using (var dbCommand = con.CreateCommand())
                    {
                        dbCommand.CommandText = " SELECT COUNT(*) FROM Orders";
                        Assert.Equal(1, dbCommand.ExecuteScalar());
                        dbCommand.CommandText = " SELECT OrderLinesCount FROM Orders";
                        Assert.Equal(DBNull.Value, dbCommand.ExecuteScalar());
                    }
                }
            }
        }

        [NonLinuxFact]
        public async Task NullPropagation_WithExplicitNull()
        {
            using (var store = GetDocumentStore())
            {
                CreateRdbmsSchema(store);

                var eventSlim = new ManualResetEventSlim(false);
                var database = await GetDatabase(store.DefaultDatabase);
                database.SqlReplicationLoader.AfterReplicationCompleted += statistics =>
                {
                    if (statistics.SuccessCount != 0)
                        eventSlim.Set();
                };

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

                await SetupSqlReplication(store, @"var orderData = {
    Id: documentId,
    City: this.Address.City,
    TotalCost: 0
};
replicateToOrders(orderData);");

                eventSlim.Wait(TimeSpan.FromMinutes(5));

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

        [NonLinuxFact]
        public async Task RavenDB_3341()
        {
            using (var store = GetDocumentStore())
            {
                CreateRdbmsSchema(store);

                var eventSlim = new ManualResetEventSlim(false);
                var database = await GetDatabase(store.DefaultDatabase);
                database.SqlReplicationLoader.AfterReplicationCompleted += statistics =>
                {
                    if (statistics.SuccessCount != 0)
                        eventSlim.Set();
                };

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

                await SetupSqlReplication(store, "if(this.OrderLines.length > 0) { \r\n" + defaultScript + " \r\n}");

                eventSlim.Wait(TimeSpan.FromMinutes(5));

                AssertCounts(1, 1, store);

                eventSlim.Reset();
                using (var session = store.OpenAsyncSession())
                {
                    var order = await session.LoadAsync<Order>("orders/1");
                    order.OrderLines.Clear();
                    await session.SaveChangesAsync();
                }
                eventSlim.Wait(TimeSpan.FromMinutes(5));
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

        [NonLinuxFact]
        public async Task CanUpdateToBeNoItemsInChildTable()
        {
            using (var store = GetDocumentStore())
            {
                CreateRdbmsSchema(store);

                var eventSlim = new ManualResetEventSlim(false);
                var database = await GetDatabase(store.DefaultDatabase);
                database.SqlReplicationLoader.AfterReplicationCompleted += statistics =>
                {
                    if (statistics.SuccessCount != 0)
                        eventSlim.Set();
                };

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

                await SetupSqlReplication(store, defaultScript);

                eventSlim.Wait(TimeSpan.FromMinutes(5));

                AssertCounts(1, 2, store);

                eventSlim.Reset();

                using (var session = store.OpenAsyncSession())
                {
                    var order = await session.LoadAsync<Order>("orders/1");
                    order.OrderLines.Clear();
                    await session.SaveChangesAsync();
                }

                eventSlim.Wait(TimeSpan.FromMinutes(5));
                AssertCounts(1, 0, store);
            }
        }

        [NonLinuxFact]
        public async Task CanDelete()
        {
            using (var store = GetDocumentStore())
            {
                CreateRdbmsSchema(store);

                var eventSlim = new ManualResetEventSlim(false);
                var database = await GetDatabase(store.DefaultDatabase);
                database.SqlReplicationLoader.AfterReplicationCompleted += statistics =>
                {
                    if (statistics.SuccessCount != 0)
                        eventSlim.Set();
                };

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

                await SetupSqlReplication(store, defaultScript);

                eventSlim.Wait(TimeSpan.FromMinutes(5));

                AssertCounts(1, 2, store);

                eventSlim.Reset();

                using (var commands = store.Commands())
                    await commands.DeleteAsync("orders/1", null);

                eventSlim.Wait(TimeSpan.FromMinutes(5));
                AssertCounts(0, 0, store);
            }
        }

        [NonLinuxFact]
        public async Task RavenDB_3172()
        {
            using (var store = GetDocumentStore())
            {
                CreateRdbmsSchema(store);

                var eventSlim = new ManualResetEventSlim(false);
                var database = await GetDatabase(store.DefaultDatabase);
                database.SqlReplicationLoader.AfterReplicationCompleted += statistics =>
                {
                    if (statistics.SuccessCount != 0)
                        eventSlim.Set();
                };

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

                await SetupSqlReplication(store, defaultScript, insertOnly: true);

                eventSlim.Wait(TimeSpan.FromMinutes(5));

                AssertCounts(1, 2, store);

                eventSlim.Reset();

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

                eventSlim.Wait(TimeSpan.FromMinutes(5));
                // we end up with duplicates
                AssertCounts(2, 5, store);
            }
        }

        [NonLinuxFact]
        public async Task WillLog()
        {
            using (var client = new ClientWebSocket())
            using (var store = GetDocumentStore())
            {
                CreateRdbmsSchema(store);

                var eventSlim = new ManualResetEventSlim(false);
                var database = await GetDatabase(store.DefaultDatabase);
                database.SqlReplicationLoader.AfterReplicationCompleted += statistics =>
                {
                    if (statistics.LastReplicatedEtag > 0)
                        eventSlim.Set();
                };

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order());
                    await session.SaveChangesAsync();
                }
                string str = string.Format("{0}/admin/logs/watch", store.Url.Replace("http", "ws"));
                StringBuilder sb = new StringBuilder();
                await client.ConnectAsync(new Uri(str), CancellationToken.None);
                var task = Task.Run((Func<Task>)(async () =>
               {
                   ArraySegment<byte> buffer = new ArraySegment<byte>(new byte[1024]);
                   while (client.State == WebSocketState.Open)
                   {
                       var value = await ReadFromWebSocket(buffer, client);
                       sb.AppendLine(value);
                       if (value.Contains("skipping document: orders/1"))
                           return;
                   }
               }));
                await SetupSqlReplication(store, @"output ('Tralala');asdfsadf
var nameArr = this.StepName.split('.');");

                Assert.True(eventSlim.Wait(TimeSpan.FromSeconds(30)));
                Assert.True(await task.WaitWithTimeout(TimeSpan.FromSeconds(30)));

                var msg = "Could not process SQL Replication script for OrdersAndLines, skipping document: orders/1";
                if (sb.ToString().Split(new string[] { Environment.NewLine }, StringSplitOptions.None).ToList().Any(x => x.Contains(msg)) == false)
                    throw new InvalidOperationException("Got bad message. Full log is: \r\n" + sb);
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

        protected static async Task SetupSqlReplication(DocumentStore store, string script, bool insertOnly = false)
        {
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new SqlConnections
                {
                    Id = Constants.SqlReplication.SqlReplicationConnections,
                    Connections =
                    {
                        ["Ci1"] = new PredefinedSqlConnection
                        {
                            ConnectionString = GetConnectionString(store),
                            FactoryName = "System.Data.SqlClient",
                        }
                    }
                });
                await session.StoreAsync(new SqlReplicationConfiguration
                {
                    Id = Constants.SqlReplication.SqlReplicationConfigurationPrefix + "OrdersAndLines",
                    Name = "OrdersAndLines",
                    ConnectionStringName = "Ci1",
                    Collection = "Orders",
                    SqlReplicationTables =
                    {
                        new SqlReplicationTable {TableName = "Orders", DocumentKeyColumn = "Id", InsertOnlyMode = insertOnly},
                        new SqlReplicationTable {TableName = "OrderLines", DocumentKeyColumn = "OrderId", InsertOnlyMode = insertOnly},
                    },
                    Script = script
                });
                await session.SaveChangesAsync();
            }
        }

        private static string GetConnectionString(DocumentStore store)
        {
            return _masterDatabaseConnection.Value + $";Initial Catalog=SqlReplication-{store.DefaultDatabase};";
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


}
