extern alias database;

using System;
using System.ComponentModel.Composition.Hosting;
using System.Configuration;
using System.Data.Common;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Reflection;
using Raven.Abstractions.Indexing;
using Raven.Bundles.IndexReplication;
using Raven.Bundles.IndexReplication.Data;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Server;
using Xunit;

namespace Raven.Bundles.Tests.IndexReplication.Bugs
{
	public class Micro : IDisposable
	{
		private readonly EmbeddableDocumentStore documentStore;

		public Micro()
		{
			documentStore = new EmbeddableDocumentStore
				{
					RunInMemory = true,
					Configuration =
						{
							Catalog =
								{
									Catalogs =
										{
											new AssemblyCatalog(typeof (IndexReplicationIndexUpdateTrigger).Assembly)
										}

								}
						}
				};
			documentStore.Initialize();
			CreateRdbmsSchema();
			CreateTestData(documentStore);

			new IR_OrderItem().Execute(documentStore);
		}

		private ConnectionStringSettings ConnectionString
		{
			get { return FactIfSqlServerIsAvailable.ConnectionStringSettings; }
		}

		private void CreateRdbmsSchema()
		{
			var providerFactory = DbProviderFactories.GetFactory(ConnectionString.ProviderName);
			using (var con = providerFactory.CreateConnection())
			{
				con.ConnectionString = ConnectionString.ConnectionString;
				con.Open();

				using (var dbCommand = con.CreateCommand())
				{
					dbCommand.CommandText = @"
IF OBJECT_ID('OrderItem') is not null 
	DROP TABLE [dbo].[OrderItem]
";
					dbCommand.ExecuteNonQuery();

					dbCommand.CommandText = @"
CREATE TABLE [dbo].[OrderItem]
(
	[DocumentId] [nvarchar](50) NOT NULL,
	[OrderItemId] [nvarchar](50) NOT NULL,
	[Description] [nvarchar](255) NOT NULL
)
";
					dbCommand.ExecuteNonQuery();
				}
			}
		}

		public void Dispose()
		{
			documentStore.Dispose();
		}

		[FactIfSqlServerIsAvailable]
		public void CanReplicateOrderItems()
		{
			using (var session = documentStore.OpenSession())
			{
				session.Advanced.LuceneQuery<object, IR_OrderItem>().WaitForNonStaleResults().ToList();
			}

			var providerFactory = DbProviderFactories.GetFactory(ConnectionString.ProviderName);
			using (var con = providerFactory.CreateConnection())
			{
				con.ConnectionString = ConnectionString.ConnectionString;

				con.Open();
				var dbCommand = con.CreateCommand();
				dbCommand.CommandText = "select count(*) from OrderItem";
				var results = dbCommand.ExecuteScalar();
				con.Close();
				Assert.Equal(3, results);
			}
		}

		public class Order
		{
			public string Id { get; set; }
			public OrderItem[] Items { get; set; }
		}

		public class OrderItem
		{
			public string Id { get; set; }
			public string Description { get; set; }
		}

		public class IR_OrderItem : AbstractIndexCreationTask<Order, OrderItem>
		{

			public IR_OrderItem()
			{
				Map = orders => from order in orders
								from item in order.Items
								select new
								{
									item.Description,
									OrderItemId = item.Id
								};
				StoreAllFields(FieldStorage.Yes);
			}

			public static IndexReplicationDestination Create_IR_Mapping()
			{
				//Create replication
				//The target table is OrderItem {OrderItemId, Description}
				var ip = new IndexReplicationDestination
					{
						Id = "Raven/IndexReplication/IR/OrderItem",
						ColumnsMapping =
							{
								{"Description", "Description"},
								{"OrderItemId", "OrderItemId"},
							},
						ConnectionStringName = FactIfSqlServerIsAvailable.ConnectionStringSettings.Name,
						PrimaryKeyColumnName = "DocumentId",
						TableName = "OrderItem"
					};

				return ip;
			}
		}

		private void CreateTestData(IDocumentStore docStore)
		{
			using (var session = docStore.OpenSession())
			{
				var order1 = new Order
				{
					Id = "orders/1",
					Items = new OrderItem[]
					{
						new OrderItem{Id = "orderitems/1", Description="item1"},
						new OrderItem{Id = "orderitems/2", Description="item2"},
					}
				};

				var order2 = new Order
				{
					Id = "orders/2",
					Items = new OrderItem[]
					{
						new OrderItem{Id = "orderitems/3", Description="item3"},
					}
				};

				session.Store(order1);
				session.Store(order2);
				session.SaveChanges();

				//IR doc
				Raven.Bundles.IndexReplication.Data.IndexReplicationDestination ip =
					IR_OrderItem.Create_IR_Mapping();
				session.Store(ip);
				session.SaveChanges();
			}

		}
	}
}