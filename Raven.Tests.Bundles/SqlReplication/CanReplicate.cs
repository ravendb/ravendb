// -----------------------------------------------------------------------
//  <copyright file="CanReplicate.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Database;
using Raven.Database.Bundles.SqlReplication;
using Raven.Database.Util;
using Raven.Json.Linq;
using Raven.Tests.Common;
using Raven.Tests.Common.Attributes;

using Xunit;
using Xunit.Extensions;
using Xunit.Sdk;

namespace Raven.Tests.Bundles.SqlReplication
{
    public class CanReplicate : RavenTest
    {
        protected override void ModifyConfiguration(Database.Config.InMemoryRavenConfiguration configuration)
        {
            configuration.Settings["Raven/ActiveBundles"] = "sqlReplication";
        }

        private const string defaultScript = @"
var orderData = {
	Id: documentId,
	OrderLinesCount: this.OrderLines.length,
	TotalCost: 0,
    Quantities: {$ArrayType: 'System.Int32', Values: this.OrderLines.map(function(l) {return l.Quantity;})}
};
replicateToorders(orderData);

for (var i = 0; i < this.OrderLines.length; i++) {
	var line = this.OrderLines[i];
	orderData.TotalCost += line.Cost;
	replicateToorderlines({
		OrderId: documentId,
		Qty: line.Quantity,
		Product: line.Product,
		Cost: line.Cost
	});
}";


        private void CreateRdbmsSchema()
        {
            var providerFactory = DbProviderFactories.GetFactory(MaybeSqlServerIsAvailable.ConnectionStringSettings.ProviderName);
            using (var con = providerFactory.CreateConnection())
            {
                con.ConnectionString = MaybeSqlServerIsAvailable.ConnectionStringSettings.ConnectionString;
                con.Open();

                using (var dbCommand = con.CreateCommand())
                {
                    dbCommand.CommandText = @"

DROP TABLE If EXISTS orderlines;
DROP TABLE If EXISTS orders;
";
                    dbCommand.ExecuteNonQuery();

                    dbCommand.CommandText = @"
CREATE TABLE orderlines
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
    ""Quantities"" int[] NULL
);";
                    dbCommand.ExecuteNonQuery();
                }
            }
        }

        [Fact]
        public void SimpleTransformation()
        {
            CreateRdbmsSchema();
            using (var store = NewDocumentStore())
            {
                var eventSlim = new ManualResetEventSlim(false);
                store.SystemDatabase.StartupTasks.OfType<SqlReplicationTask>()
                    .First().AfterReplicationCompleted += successCount =>
                    {
                        if (successCount != 0)
                            eventSlim.Set();
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

                SetupSqlReplication(store, defaultScript);

                eventSlim.Wait(TimeSpan.FromMinutes(5));

                var providerFactory = DbProviderFactories.GetFactory(MaybeSqlServerIsAvailable.ConnectionStringSettings.ProviderName);
                using (var con = providerFactory.CreateConnection())
                {
                    con.ConnectionString = MaybeSqlServerIsAvailable.ConnectionStringSettings.ConnectionString;
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
        public void NullPropagation()
        {
            CreateRdbmsSchema();
            using (var store = NewDocumentStore())
            {
                var eventSlim = new ManualResetEventSlim(false);
                store.SystemDatabase.StartupTasks.OfType<SqlReplicationTask>()
                    .First().AfterReplicationCompleted += successCount =>
                    {
                        if (successCount != 0)
                            eventSlim.Set();
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

                SetupSqlReplication(store, @"var orderData = {
    Id: documentId,
    OrderLinesCount: this.OrderLines_Missing.length,
    TotalCost: 0
};
replicateToOrders(orderData);");

                eventSlim.Wait(TimeSpan.FromMinutes(5));

                var providerFactory = DbProviderFactories.GetFactory(MaybeSqlServerIsAvailable.ConnectionStringSettings.ProviderName);
                using (var con = providerFactory.CreateConnection())
                {
                    con.ConnectionString = MaybeSqlServerIsAvailable.ConnectionStringSettings.ConnectionString;
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
        public void NullPropagation_WithExplicitNull()
        {
            CreateRdbmsSchema();
            using (var store = NewDocumentStore())
            {
                var eventSlim = new ManualResetEventSlim(false);
                store.SystemDatabase.StartupTasks.OfType<SqlReplicationTask>()
                    .First().AfterReplicationCompleted += successCount =>
                    {
                        if (successCount != 0)
                            eventSlim.Set();
                    };

                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        Address = null,
                        OrderLines = new List<OrderLine>
                        {
                            new OrderLine{Cost = 3, Product = "Milk", Quantity = 3},
                            new OrderLine{Cost = 4, Product = "Bear", Quantity = 2},
                        }
                    });
                    session.SaveChanges();
                }

                SetupSqlReplication(store, @"var orderData = {
    Id: documentId,
    City: this.Address.City,
    TotalCost: 0
};
replicateToOrders(orderData);");

                eventSlim.Wait(TimeSpan.FromMinutes(5));

                var providerFactory = DbProviderFactories.GetFactory(MaybeSqlServerIsAvailable.ConnectionStringSettings.ProviderName);
                using (var con = providerFactory.CreateConnection())
                {
                    con.ConnectionString = MaybeSqlServerIsAvailable.ConnectionStringSettings.ConnectionString;
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
        public void ReplicateMultipleBatches()
        {
            CreateRdbmsSchema();
            using (var store = NewDocumentStore())
            {
                var eventSlim = new ManualResetEventSlim(false);

                int testCount = 5000;
                store.SystemDatabase.StartupTasks.OfType<SqlReplicationTask>()
                    .First().AfterReplicationCompleted += successCount =>
                    {
                        if (GetOrdersCount() == testCount)
                            eventSlim.Set();
                    };

                using (var session = store.BulkInsert())
                {
                    for (int i = 0; i < testCount; i++)
                    {
                        session.Store(new Order
                                      {
                                          OrderLines = new List<OrderLine>
                                                       {
                                                           new OrderLine {Cost = 3, Product = "Milk", Quantity = 3},
                                                           new OrderLine {Cost = 4, Product = "Bear", Quantity = 2},
                                                       }

                                      });
                    }
                }

                SetupSqlReplication(store, defaultScript);

                eventSlim.Wait(TimeSpan.FromMinutes(5));

                Assert.Equal(testCount, GetOrdersCount());

            }
        }

        [Fact]
        public void RavenDB_3341()
        {
            CreateRdbmsSchema();
            using (var store = NewDocumentStore())
            {
                var eventSlim = new ManualResetEventSlim(false);
                store.SystemDatabase.StartupTasks.OfType<SqlReplicationTask>()
                    .First().AfterReplicationCompleted += successCount =>
                    {
                        if (successCount != 0)
                            eventSlim.Set();
                    };

                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        OrderLines = new List<OrderLine>
                        {
                            new OrderLine{Cost = 4, Product = "Bear", Quantity = 2},
                        }
                    });

                    session.SaveChanges();
                }

                SetupSqlReplication(store, "if(this.OrderLines.length > 0) { \r\n" + defaultScript + " \r\n}");

                eventSlim.Wait(TimeSpan.FromMinutes(5));
                
                AssertCounts(1, 1);

                eventSlim.Reset();
                using (var session = store.OpenSession())
                {
                    var order = session.Load<Order>(1);
                    order.OrderLines.Clear();
                    session.SaveChanges();
                }
                eventSlim.Wait(TimeSpan.FromMinutes(5));
                AssertCounts(0, 0);
            }
        }
        private static int GetOrdersCount()
        {
            var providerFactory =
                    DbProviderFactories.GetFactory(MaybeSqlServerIsAvailable.ConnectionStringSettings.ProviderName);
            using (var con = providerFactory.CreateConnection())
            {
                con.ConnectionString = MaybeSqlServerIsAvailable.ConnectionStringSettings.ConnectionString;
                con.Open();

                using (var dbCommand = con.CreateCommand())
                {
                    dbCommand.CommandText = " SELECT COUNT(*) FROM Orders";
                    return (int)dbCommand.ExecuteScalar();
                }
            }
        }

        protected override void CreateDefaultIndexes(Client.IDocumentStore documentStore)
        {

        }

        [Fact]
        public void CanUpdateToBeNoItemsInChildTable()
        {
            CreateRdbmsSchema();
            using (var store = NewDocumentStore())
            {
                var eventSlim = new ManualResetEventSlim(false);
                store.SystemDatabase.StartupTasks.OfType<SqlReplicationTask>()
                     .First().AfterReplicationCompleted += successCount =>
                     {
                         if (successCount != 0)
                             eventSlim.Set();
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

                SetupSqlReplication(store, defaultScript);

                eventSlim.Wait(TimeSpan.FromMinutes(5));

                AssertCounts(1, 2);

                eventSlim.Reset();

                using (var session = store.OpenSession())
                {
                    session.Load<Order>(1).OrderLines.Clear();
                    session.SaveChanges();
                }

                eventSlim.Wait(TimeSpan.FromMinutes(5));

                AssertCounts(1, 0);

            }
        }

        [Fact]
        public void CanDelete()
        {
            CreateRdbmsSchema();
            using (var store = NewDocumentStore())
            {
                var eventSlim = new ManualResetEventSlim(false);
                var sqlReplicationTask = store.SystemDatabase.StartupTasks.OfType<SqlReplicationTask>().First();
                sqlReplicationTask
                     .AfterReplicationCompleted += successCount =>
                     {
                         if (successCount != 0)
                             eventSlim.Set();
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

                SetupSqlReplication(store, defaultScript);

                eventSlim.Wait(TimeSpan.FromMinutes(5));

                Alert lastAlert;
                if (sqlReplicationTask.Statistics.ContainsKey("OrdersAndLines"))
                {
                    lastAlert = sqlReplicationTask.Statistics["OrdersAndLines"].LastAlert;
                    if (lastAlert != null)
                        throw new AssertActualExpectedException(null, lastAlert, lastAlert.Message + lastAlert.Exception);
                }

                AssertCounts(1, 2);

                eventSlim.Reset();

                store.DatabaseCommands.Delete("orders/1", null);

                eventSlim.Wait(TimeSpan.FromMinutes(5));

                AssertCounts(0, 0);

            }
        }
        [Fact]
        public void CanRep()
        {
            CreateRdbmsSchema();
            using (var store = NewDocumentStore())
            {
                var eventSlim = new ManualResetEventSlim(false);
                var sqlReplicationTask = store.SystemDatabase.StartupTasks.OfType<SqlReplicationTask>().First();
                sqlReplicationTask
                     .AfterReplicationCompleted += successCount =>
                     {
                         if (successCount != 0)
                             eventSlim.Set();
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

                SetupSqlReplication(store, defaultScript);

                eventSlim.Wait(TimeSpan.FromMinutes(5));

                Alert lastAlert;
                if (sqlReplicationTask.Statistics.ContainsKey("OrdersAndLines"))
                {
                    lastAlert = sqlReplicationTask.Statistics["OrdersAndLines"].LastAlert;
                    if (lastAlert != null)
                        throw new AssertActualExpectedException(null, lastAlert, lastAlert.Message + lastAlert.Exception);
                }

                AssertCounts(1, 2);
            }
        }

        [Fact]
        public void CanReplicateEmptyList()
        {
            CreateRdbmsSchema();
            using (var store = NewDocumentStore())
            {
                var eventSlim = new ManualResetEventSlim(false);
                var sqlReplicationTask = store.SystemDatabase.StartupTasks.OfType<SqlReplicationTask>().First();
                sqlReplicationTask
                     .AfterReplicationCompleted += successCount =>
                     {
                         if (successCount != 0)
                             eventSlim.Set();
                     };

                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        OrderLines = new List<OrderLine>()
                    });
                    session.SaveChanges();
                }

                SetupSqlReplication(store, defaultScript);

                eventSlim.Wait(TimeSpan.FromMinutes(5));

                Alert lastAlert;
                if (sqlReplicationTask.Statistics.ContainsKey("OrdersAndLines"))
                {
                    lastAlert = sqlReplicationTask.Statistics["OrdersAndLines"].LastAlert;
                    if (lastAlert != null)
                        throw new AssertActualExpectedException(null, lastAlert, lastAlert.Message + lastAlert.Exception);
                }

                AssertCounts(1, 0);

                eventSlim.Reset();

                store.DatabaseCommands.Delete("orders/1", null);

                eventSlim.Wait(TimeSpan.FromMinutes(5));

                AssertCounts(0, 0);

            }
        }

        [Fact]
        public void RavenDB_3172()
        {
            CreateRdbmsSchema();
            using (var store = NewDocumentStore())
            {
                var eventSlim = new ManualResetEventSlim(false);
                store.SystemDatabase.StartupTasks.OfType<SqlReplicationTask>()
                     .First().AfterReplicationCompleted += successCount =>
                     {
                         if (successCount != 0)
                             eventSlim.Set();
                     };

                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        Id = "orders/1",
                        OrderLines = new List<OrderLine>
                        {
                            new OrderLine{Cost = 3, Product = "Milk", Quantity = 3},
                            new OrderLine{Cost = 4, Product = "Bear", Quantity = 2},
                        }
                    });
                    session.SaveChanges();
                }

                SetupSqlReplication(store, defaultScript, insertOnly: true);

                eventSlim.Wait(TimeSpan.FromMinutes(5));

                AssertCounts(1, 2);

                eventSlim.Reset();

                using (var session = store.OpenSession())
                {
                    var order = session.Load<Order>("orders/1");
                    order.OrderLines.Add(new OrderLine
                    {
                        Cost = 5,
                        Product = "Sugar",
                        Quantity = 7
                    });
                    session.SaveChanges();
                }

                eventSlim.Wait(TimeSpan.FromMinutes(5));

                // we end up with duplicates
                AssertCounts(2, 5);
            }
        }

        [Fact]
        public void WillLog()
        {
            LogManager.RegisterTarget<DatabaseMemoryTarget>();

            CreateRdbmsSchema();
            using (var store = NewDocumentStore())
            {
                var eventSlim = new ManualResetEventSlim(false);
                store.SystemDatabase.StartupTasks.OfType<SqlReplicationTask>()
                     .First().AfterReplicationCompleted += successCount =>
                     {
                         if (successCount != 0)
                             eventSlim.Set();
                     };

                using (var session = store.OpenSession())
                {
                    session.Store(new Order());
                    session.SaveChanges();
                }

                SetupSqlReplication(store, @"output ('Tralala');asdfsadf
var nameArr = this.StepName.split('.');");

                Assert.True(eventSlim.Wait(TimeSpan.FromSeconds(30)));

                var databaseMemoryTarget = LogManager.GetTarget<DatabaseMemoryTarget>();
                var warnLog = databaseMemoryTarget[Constants.SystemDatabase].WarnLog;
                var msg = "Could not process SQL Replication script for OrdersAndLines, skipping document: orders/1";


                if (warnLog.Any(x=>x.FormattedMessage.Contains(msg)) == false)
                    throw new InvalidOperationException("Got bad message. Full warn log is: \r\n" + String.Join(Environment.NewLine, databaseMemoryTarget[Constants.SystemDatabase].WarnLog.Select(x=>x.FormattedMessage)));
            }
        }

        [Fact]
        public void RavenDB_3106()
        {
            CreateRdbmsSchema();
            using (var store = NewDocumentStore())
            {
                new RavenDocumentsByEntityName().Execute(store);

                var eventSlim = new ManualResetEventSlim(false);
                store.DocumentDatabase.StartupTasks.OfType<SqlReplicationTask>()
                     .First().AfterReplicationCompleted += successCount =>
                     {
                         if (successCount != 0)
                             eventSlim.Set();
                     };

                using (var session = store.OpenSession())
                {
                    for (var i = 0; i < 2048; i++)
                    {
                        session.Store(new Order
                        {
                            OrderLines = new List<OrderLine>
                            {
                                new OrderLine{Cost = 3, Product = "Milk", Quantity = 3},
                                new OrderLine{Cost = 4, Product = "Bear", Quantity = 2},
                            }
                        });
                    }

                    session.SaveChanges();
                }

                SetupSqlReplication(store, defaultScript);

                eventSlim.Wait(TimeSpan.FromMinutes(5));

                AssertCountsWithTimeout(2048, 4096, TimeSpan.FromMinutes(1));

                eventSlim.Reset();

                PauseReplication(0, store.DocumentDatabase);

                WaitForIndexing(store);

                store
                    .DatabaseCommands
                    .DeleteByIndex("Raven/DocumentsByEntityName", new IndexQuery { Query = "Tag:Orders OR Tag:OrderLines" })
                    .WaitForCompletion();

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

                AssertCountsWithTimeout(1, 2, TimeSpan.FromMinutes(1));
            }
        }

        private static void AssertCountsWithTimeout(int ordersCount, int orderLineCounts, TimeSpan timeout)
        {
            Exception lastException = null;

            var stopWatch = Stopwatch.StartNew();
            while (stopWatch.Elapsed <= timeout)
            {
                try
                {
                    AssertCounts(ordersCount, orderLineCounts);
                    return;
                }
                catch (AssertException e)
                {
                    lastException = e;
                }

                Thread.Sleep(500);
            }

            throw lastException;
        }

        private static void AssertCounts(long ordersCount, long orderLineCounts)
        {
            var providerFactory = DbProviderFactories.GetFactory(MaybeSqlServerIsAvailable.ConnectionStringSettings.ProviderName);
            using (var con = providerFactory.CreateConnection())
            {
                con.ConnectionString = MaybeSqlServerIsAvailable.ConnectionStringSettings.ConnectionString;
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

        private static void SetupSqlReplication(EmbeddableDocumentStore store, string script, bool insertOnly = false)
        {
            using (var session = store.OpenSession())
            {
                session.Store(new SqlReplicationConfig
                {
                    Id = "Raven/SqlReplication/Configuration/OrdersAndLines",
                    Name = "OrdersAndLines",
                    ConnectionString = MaybeSqlServerIsAvailable.ConnectionStringSettings.ConnectionString,
                    FactoryName = MaybeSqlServerIsAvailable.ConnectionStringSettings.ProviderName,
                    RavenEntityName = "Orders",
                    SqlReplicationTables =
					{
						new SqlReplicationTable {TableName = "orders", DocumentKeyColumn = "Id", InsertOnlyMode = insertOnly},
						new SqlReplicationTable {TableName = "orderlines", DocumentKeyColumn = "OrderId", InsertOnlyMode = insertOnly},
					},
                    Script = script
                });
                session.SaveChanges();
            }
        }

        protected void PauseReplication(int serverIndex, DocumentDatabase database, bool waitToStop = true)
        {
            var replicationTask = database.StartupTasks.OfType<SqlReplicationTask>().First();

            replicationTask.Pause();

            if (waitToStop)
                SpinWait.SpinUntil(() => replicationTask.IsRunning == false, TimeSpan.FromSeconds(10));
        }

        protected void ContinueReplication(int serverIndex, DocumentDatabase database, bool waitToStart = true)
        {
            var replicationTask = database.StartupTasks.OfType<SqlReplicationTask>().First();

            replicationTask.Continue();

            if (waitToStart)
                SpinWait.SpinUntil(() => replicationTask.IsRunning, TimeSpan.FromSeconds(10));
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
