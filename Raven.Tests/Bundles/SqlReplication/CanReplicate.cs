// -----------------------------------------------------------------------
//  <copyright file="CanReplicate.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading;
using Raven.Database.Bundles.SqlReplication;
using Xunit;

namespace Raven.Tests.Bundles.SqlReplication
{
	public class CanReplicate : RavenTest
	{
		protected override void ModifyConfiguration(Database.Config.RavenConfiguration configuration)
		{
			configuration.Settings["Raven/ActiveBundles"] = "sqlReplication";
		}

		private void CreateRdbmsSchema()
		{
			var providerFactory = DbProviderFactories.GetFactory(FactIfSqlServerIsAvailable.ConnectionStringSettings.ProviderName);
			using (var con = providerFactory.CreateConnection())
			{
				con.ConnectionString = FactIfSqlServerIsAvailable.ConnectionStringSettings.ConnectionString;
				con.Open();

				using (var dbCommand = con.CreateCommand())
				{
					dbCommand.CommandText = @"
IF OBJECT_ID('Orders') is not null 
	DROP TABLE [dbo].[Orders]
IF OBJECT_ID('OrderLines') is not null 
	DROP TABLE [dbo].[OrderLines]
";
					dbCommand.ExecuteNonQuery();

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
	[OrderLinesCount] [int] NOT NULL,
	[TotalCost] [int] NOT NULL
)
";
					dbCommand.ExecuteNonQuery();
				}
			}
		}


		[FactIfSqlServerIsAvailable]
		public void SimpleTransformation()
		{
			CreateRdbmsSchema();
			using (var store = NewDocumentStore())
			{
				var eventSlim = new ManualResetEventSlim(false);
				store.DocumentDatabase.StartupTasks.OfType<SqlReplicationTask>()
					 .First().AfterReplicationCompleted += () => eventSlim.Set();
				
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

				using (var session = store.OpenSession())
				{
					session.Store(new SqlReplicationConfig
					{
						Id = "Raven/SqlReplication/Configuration/OrdersAndLines",
						Name = "OrdersAndLines",
						ConnectionString = FactIfSqlServerIsAvailable.ConnectionStringSettings.ConnectionString,
						FactoryName = FactIfSqlServerIsAvailable.ConnectionStringSettings.ProviderName,
						RavenEntityName = "Orders",
						Script = @"
var orderData = {
	Id: documentId,
	OrderLinesCount: this.OrderLines.length,
	TotalCost: 0
};
sqlReplicate('Orders', 'Id', orderData);

for (var i = 0; i < this.OrderLines.length; i++) {
	var line = this.OrderLines[i];
	orderData.TotalCost += line.Cost;
	sqlReplicate('OrderLines','OrderId', {
		OrderId: documentId,
		Qty: line.Quantity,
		Product: line.Product,
		Cost: line.Cost
	});
}"
					});
					session.SaveChanges();
				}

				eventSlim.Wait(TimeSpan.FromMinutes(5));

				var providerFactory = DbProviderFactories.GetFactory(FactIfSqlServerIsAvailable.ConnectionStringSettings.ProviderName);
				using (var con = providerFactory.CreateConnection())
				{
					con.ConnectionString = FactIfSqlServerIsAvailable.ConnectionStringSettings.ConnectionString;
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

		public class Order
		{
			public string Id { get; set; }
			public List<OrderLine> OrderLines { get; set; }
		}

		public class OrderLine
		{
			public string Product { get; set; }
			public int Quantity { get; set; }
			public int Cost { get; set; }
		}
	}
}