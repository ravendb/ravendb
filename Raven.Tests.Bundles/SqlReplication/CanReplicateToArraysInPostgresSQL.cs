// -----------------------------------------------------------------------
//  <copyright file="CanReplicateToArraysInPostgresSQL.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Client.Embedded;
using Raven.Database.Bundles.SqlReplication;
using Raven.Tests.Common;
using Raven.Tests.Common.Attributes;
using Xunit;
using Xunit.Sdk;

namespace Raven.Tests.Bundles.SqlReplication
{
    public class CanReplicateToArraysInPostgresSQL : RavenTest
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
    Quantities: { 
        EnumType : 'NpgsqlTypes.NpgsqlDbType, Npgsql',
        EnumValue : 'Array | Double',
        EnumProperty : 'NpgsqlDbType',
        Values : this.OrderLines.map(function(l) {return l.Quantity;})
    }
};
replicateToorders(orderData);

for (var i = 0; i < this.OrderLines.length; i++) {
	var line = this.OrderLines[i];
	orderData.TotalCost += line.Cost;
	replicateToorder_lines({
		OrderId: documentId,
		Qty: line.Quantity,
		Product: line.Product,
		Cost: line.Cost
	});
}";

        private void CreateRdbmsSchema()
        {
            var providerFactory = DbProviderFactories.GetFactory(MaybeSqlServerIsAvailable.PostgresConnectionStringSettings.ProviderName);
            using (var con = providerFactory.CreateConnection())
            {
                con.ConnectionString = MaybeSqlServerIsAvailable.PostgresConnectionStringSettings.ConnectionString;
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
    ""Quantities"" int[] NULL
);";
                    dbCommand.ExecuteNonQuery();
                }
            }
        }

        [Fact]
        public void CanReplicate()
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
                    session.Store(new CanReplicate.Order
                    {
                        OrderLines = new List<CanReplicate.OrderLine>
                        {
                            new CanReplicate.OrderLine{Cost = 3, Product = "Milk", Quantity = 3},
                            new CanReplicate.OrderLine{Cost = 4, Product = "Bear", Quantity = 2},
                        }
                    });
                    session.SaveChanges();
                }

                SetupSqlReplication(store, defaultScript);
                
                eventSlim.Wait(TimeSpan.FromMinutes(1));

                Alert lastAlert;
                if (sqlReplicationTask.Statistics.ContainsKey("OrdersAndLines"))
                {
                    lastAlert = sqlReplicationTask.Statistics["OrdersAndLines"].LastAlert;
                    if (lastAlert != null)
                        throw new AssertActualExpectedException(null, lastAlert, lastAlert.Message + lastAlert.Exception);
                }

                AssertCounts(1, 2, new [] {3, 2});
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
                    session.Store(new CanReplicate.Order
                    {
                        OrderLines = new List<CanReplicate.OrderLine>
                        {
                            new CanReplicate.OrderLine{Cost = 3, Product = "Milk", Quantity = 3},
                            new CanReplicate.OrderLine{Cost = 4, Product = "Bear", Quantity = 2},
                        }
                    });
                    session.SaveChanges();
                }

                SetupSqlReplication(store, defaultScript);

                eventSlim.Wait(TimeSpan.FromMinutes(1));

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

                eventSlim.Wait(TimeSpan.FromMinutes(1));

                AssertCounts(0, 0);

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
                    ConnectionString = MaybeSqlServerIsAvailable.PostgresConnectionStringSettings.ConnectionString,
                    FactoryName = MaybeSqlServerIsAvailable.PostgresConnectionStringSettings.ProviderName,
                    RavenEntityName = "Orders",
                    SqlReplicationTables =
                    {
                        new SqlReplicationTable {TableName = "orders", DocumentKeyColumn = "Id", InsertOnlyMode = insertOnly},
                        new SqlReplicationTable {TableName = "order_lines", DocumentKeyColumn = "OrderId", InsertOnlyMode = insertOnly},
                    },
                    Script = script
                });
                session.SaveChanges();
            }
        }

        private static void AssertCounts(long ordersCount, long orderLineCounts, int[] orderQuantities = null)
        {
            var providerFactory = DbProviderFactories.GetFactory(MaybeSqlServerIsAvailable.PostgresConnectionStringSettings.ProviderName);
            using (var con = providerFactory.CreateConnection())
            {
                con.ConnectionString = MaybeSqlServerIsAvailable.PostgresConnectionStringSettings.ConnectionString;
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
                }
            }
        }
    }
}