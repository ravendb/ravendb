// -----------------------------------------------------------------------
//  <copyright file="CanReplicate.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Client.Document;
using Raven.Database.Util;
using Raven.Server.Documents;
using Raven.Server.Documents.SqlReplication;
using Raven.Tests.Core;
using Xunit;
using Xunit.Sdk;

namespace FastTests.Server.Documents.SqlReplication
{
    public class CanReplicate : RavenTestBase
    {
        private const string defaultScript = @"
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


        private void CreateRdbmsSchema(DocumentStore store)
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
                catch (Exception)
                {
                    try
                    {
                        var readAllLines = File.ReadAllLines(@"P:\Build\SqlReplicationPassword.txt");
                        return $@"Data Source=ci1\sqlexpress;User Id={readAllLines[0]};Password={readAllLines[1]};Connection Timeout=1";
                    }
                    catch (Exception e)
                    {
                        throw new InvalidOperationException("Use a valid connection", e);
                    }
                }
            }
        });

        private static void CreateRdbmsDatabase(DocumentStore store)
        {
            using (var con = new SqlConnection())
            {
                con.ConnectionString = _masterDatabaseConnection.Value;
                con.Open();

                using (var dbCommand = con.CreateCommand())
                {
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

        [Fact]
        public async Task SimpleTransformation()
        {
            using (var store = await GetDocumentStore())
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

        [Fact]
        public async Task NullPropagation()
        {
            using (var store = await GetDocumentStore())
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

        [Fact]
        public async Task NullPropagation_WithExplicitNull()
        {
            using (var store = await GetDocumentStore())
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

        [Fact]
        public async Task ReplicateMultipleBatches()
        {
            using (var store = await GetDocumentStore())
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

        [Fact]
        public async Task RavenDB_3341()
        {
            using (var store = await GetDocumentStore())
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
                    var order = await session.LoadAsync<Order>(1);
                    order.OrderLines.Clear();
                    await session.SaveChangesAsync();
                }
                eventSlim.Wait(TimeSpan.FromMinutes(5));
                AssertCounts(0, 0, store);
            }
        }

        private static int GetOrdersCount(DocumentStore store)
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
            using (var store = await GetDocumentStore())
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
                    var order = await session.LoadAsync<Order>(1);
                    order.OrderLines.Clear();
                    await session.SaveChangesAsync();
                }

                eventSlim.Wait(TimeSpan.FromMinutes(5));
                AssertCounts(1, 0, store);
            }
        }

        [Fact]
        public async Task CanDelete()
        {
            using (var store = await GetDocumentStore())
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

                await store.AsyncDatabaseCommands.DeleteAsync("orders/1", null);

                eventSlim.Wait(TimeSpan.FromMinutes(5));
                AssertCounts(0, 0, store);
            }
        }

        [Fact]
        public async Task RavenDB_3172()
        {
            using (var store = await GetDocumentStore())
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

        [Fact(Skip = "Waiting for RavenDB-4398: Internal log")]
        public async Task WillLog()
        {
            LogManager.RegisterTarget<DatabaseMemoryTarget>();

            using (var store = await GetDocumentStore())
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

                await SetupSqlReplication(store, @"output ('Tralala');asdfsadf
var nameArr = this.StepName.split('.');");

                Assert.True(eventSlim.Wait(TimeSpan.FromSeconds(30)));

                var databaseMemoryTarget = LogManager.GetTarget<DatabaseMemoryTarget>();
                var warnLog = databaseMemoryTarget[Constants.SystemDatabase].WarnLog;
                var msg = "Could not process SQL Replication script for OrdersAndLines, skipping document: orders/1";

                if (warnLog.Any(x => x.FormattedMessage.Contains(msg)) == false)
                    throw new InvalidOperationException("Got bad message. Full warn log is: \r\n" + String.Join(Environment.NewLine, databaseMemoryTarget[Constants.SystemDatabase].WarnLog.Select(x => x.FormattedMessage)));
            }
        }

        /*
                [Fact]
                public async Task RavenDB_3106()
                {
                    using (var store = await GetDocumentStore())
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
                            for (var i = 0; i < 2048; i++)
                            {
                                await session.StoreAsync(new Order
                                {
                                    OrderLines = new List<OrderLine>
                                    {
                                        new OrderLine{Cost = 3, Product = "Milk", Quantity = 3},
                                        new OrderLine{Cost = 4, Product = "Bear", Quantity = 2},
                                    }
                                });
                            }
                            await session.SaveChangesAsync();
                        }

                        await SetupSqlReplication(store, defaultScript);

                        eventSlim.Wait(TimeSpan.FromMinutes(5));

                        AssertCountsWithTimeout(2048, 4096, TimeSpan.FromMinutes(1), store);

                        eventSlim.Reset();

                        PauseReplication(0, database);

                        WaitForIndexing(store);

                        await store.AsyncDatabaseCommands.DeleteCollectionAsync("Orders", "OrderLines");

                        WaitForIndexing(store);

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

                        ContinueReplication(0, store.DocumentDatabase);

                        eventSlim.Wait(TimeSpan.FromMinutes(5));

                        AssertCountsWithTimeout(1, 2, TimeSpan.FromMinutes(1), store);
                    }
                }
        */

        private static void AssertCountsWithTimeout(int ordersCount, int orderLineCounts, TimeSpan timeout, DocumentStore store)
        {
            Exception lastException = null;

            var stopWatch = Stopwatch.StartNew();
            while (stopWatch.Elapsed <= timeout)
            {
                try
                {
                    AssertCounts(ordersCount, orderLineCounts, store);
                    return;
                }
                catch (XunitException e)
                {
                    lastException = e;
                }

                Thread.Sleep(500);
            }

            throw lastException;
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

        private static async Task SetupSqlReplication(DocumentStore store, string script, bool insertOnly = false)
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

        public class Order
        {
            public Address Address { get; set; }
            public string Id { get; set; }
            public List<OrderLine> OrderLines { get; set; }
        }

        public class Address
        {
            public string City { get; set; }
        }

        public class OrderLine
        {
            public string Product { get; set; }
            public int Quantity { get; set; }
            public int Cost { get; set; }
        }
    }
}