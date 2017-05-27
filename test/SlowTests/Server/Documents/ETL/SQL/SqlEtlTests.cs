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
using Raven.Client.Documents.Operations;
using Raven.Client.Extensions;
using Raven.Server.Documents.ETL;
using Raven.Server.Documents.ETL.Providers.SQL;
using Raven.Server.Documents.ETL.Providers.SQL.RelationalWriters;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Platform;
using Xunit;

namespace SlowTests.Server.Documents.ETL.SQL
{
    public class SqlEtlTests : EtlTestBase
    {
        private static readonly Lazy<string> _masterDatabaseConnection = new Lazy<string>(() =>
        {
            var local = @"Data Source=localhost\sqlexpress;Integrated Security=SSPI;Connection Timeout=3";
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
                    local = @"Data Source=ci1\sqlexpress;Integrated Security=SSPI;Connection Timeout=3";
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
                        local = @"Data Source=(localdb)\v11.0;Integrated Security=SSPI;Connection Timeout=3";
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
    Id: __document_id,
    OrderLinesCount: this.OrderLines.length,
    TotalCost: 0
};

for (var i = 0; i < this.OrderLines.length; i++) {
    var line = this.OrderLines[i];
    orderData.TotalCost += line.Cost;
    loadToOrderLines({
        OrderId: __document_id,
        Qty: line.Quantity,
        Product: line.Product,
        Cost: line.Cost
    });
}

loadToOrders(orderData);
";

        [NonLinuxFact]
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

        [NonLinuxFact]
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

        [NonLinuxFact]
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
    Id: __document_id,
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
    Id: __document_id,
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

        [NonLinuxFact]
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

        [NonLinuxFact]
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

        [NonLinuxFact]
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

        [NonLinuxFact]
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

        [NonLinuxFact]
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
                string str = string.Format("{0}/admin/logs/watch", store.Urls.First().Replace("http", "ws"));
                StringBuilder sb = new StringBuilder();
                await client.ConnectAsync(new Uri(str), CancellationToken.None);
                var task = Task.Run((Func<Task>)(async () =>
               {
                   ArraySegment<byte> buffer = new ArraySegment<byte>(new byte[1024]);
                   while (client.State == WebSocketState.Open)
                   {
                       var value = await ReadFromWebSocket(buffer, client);
                       lock (sb)
                       {
                           sb.AppendLine(value);
                       }
                       const string expectedValue = "skipping document: orders/1";
                       if (value.Contains(expectedValue) || sb.ToString().Contains(expectedValue))
                           return;

                   }
               }));
                SetupSqlEtl(store, @"output ('Tralala');asdfsadf
var nameArr = this.StepName.split('.'); loadToOrders({});");

                var condition = await task.WaitWithTimeout(TimeSpan.FromSeconds(30));
                if (condition == false)
                {
                    var msg = "Could not process SQL Replication script for OrdersAndLines, skipping document: orders/1";
                    var tempFileName = Path.GetTempFileName();
                    File.WriteAllText(tempFileName, sb.ToString());
                    throw new InvalidOperationException($"{msg}. Full log is: \r\n{tempFileName}");
                }
            }
        }

        [NonLinuxFact]
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

                var database = GetDatabase(store.Database).Result;

                DocumentsOperationContext context;
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
                using (context.OpenReadTransaction())
                {
                    for (int i = 0; i < 2; i++)
                    {
                        var result = SqlEtl.SimulateSqlEtl(new SimulateSqlEtl
                        {
                            PerformRolledBackTransaction = i % 2 != 0,
                            DocumentId = "orders/1-A",
                            Configuration = new EtlConfiguration<SqlDestination>()
                            {
                                Destination = new SqlDestination
                                {
                                    Connection = new SqlEtlConnection
                                    {
                                        ConnectionString = GetConnectionString(store),
                                        FactoryName = "System.Data.SqlClient",
                                    },
                                    SqlTables =
                                    {
                                        new SqlEtlTable {TableName = "Orders", DocumentIdColumn = "Id"},
                                        new SqlEtlTable {TableName = "OrderLines", DocumentIdColumn = "OrderId"},
                                    }
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
                        }, database, context);

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

        [NonLinuxFact]
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
    Id: __document_id,
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

        [NonLinuxFact]
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

                SetupEtl(store, new EtlDestinationsConfig
                {
                    SqlDestinations =
                    {
                        new EtlConfiguration<SqlDestination>
                        {
                            Destination = new SqlDestination
                            {
                                Connection = new SqlEtlConnection()
                                {
                                    FactoryName = "System.Data.SqlClient",
                                    ConnectionString = GetConnectionString(store)
                                },
                                SqlTables =
                                {
                                    new SqlEtlTable {TableName = "Attachments", DocumentIdColumn = "UserId", InsertOnlyMode = false},
                                },
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
        UserId: __document_id,
        AttachmentName: attachments[i].Name,
        Data: loadAttachment(attachments[i].Name)
    };

    loadToAttachments(attachment);
}
"
                                }
                            }
                        }

                    }
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

        [NonLinuxFact]
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
    Id: __document_id,
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

        [NonLinuxFact]
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

                SetupSqlEtl(store, defaultScript, collections: new List<string> {"Orders", "FavouriteOrders" });

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
            SetupEtl(store, new EtlDestinationsConfig
            {
                SqlDestinations =
                {
                    new EtlConfiguration<SqlDestination>
                    {
                        Destination = new SqlDestination
                        {
                            Connection = new SqlEtlConnection()
                            {
                                FactoryName = "System.Data.SqlClient",
                                ConnectionString = GetConnectionString(store)
                            },
                            SqlTables =
                            {
                                new SqlEtlTable {TableName = "Orders", DocumentIdColumn = "Id", InsertOnlyMode = insertOnly},
                                new SqlEtlTable {TableName = "OrderLines", DocumentIdColumn = "OrderId", InsertOnlyMode = insertOnly},
                            },
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
                    }

                }
            });
        }

        private static string GetConnectionString(DocumentStore store)
        {
            return _masterDatabaseConnection.Value + $";Initial Catalog=SqlReplication-{store.Database};";
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
