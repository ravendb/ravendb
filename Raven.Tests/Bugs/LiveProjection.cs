//-----------------------------------------------------------------------
// <copyright file="LiveProjection.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using Lucene.Net.Documents;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class LiveProjection : RavenTest, IDisposable
	{
		public LiveProjection()
		{
			Store = NewDocumentStore();
			var purchaseHistoryIndex = new PurchaseHistoryIndex();
			IDocumentStore documentStore = Store;
			purchaseHistoryIndex.Execute(documentStore.DatabaseCommands, documentStore.Conventions);
		}

		public EmbeddableDocumentStore Store { get; set; }

		[Fact]
		public void PurchaseHistoryViewReturnsExpectedData()
		{
			using (var session = Store.OpenSession())
			{
				session.Store(new Product() { Id = "product1", Name = "one" });
				session.Store(new Product() { Id = "product2", Name = "two" });
				session.Store(new Product() { Id = "product3", Name = "three" });

				session.Store(new Shipment()
				{
					UserId = "user1",
					Items = new List<ShipmentItem>()
					{
						new ShipmentItem(){ ProductId = "product1"}
					}
				});
				session.Store(new Shipment()
				{
					UserId = "user1",
					Items = new List<ShipmentItem>()
					{
						new ShipmentItem(){ ProductId = "product2"}
					}
				});
				session.Store(new Shipment()
				{
					UserId = "user2",
					Items = new List<ShipmentItem>()
					{
						new ShipmentItem(){ ProductId = "product3"}
					}
				});

				session.SaveChanges();

				session.Query<Shipment, PurchaseHistoryIndex>().Customize(x => x.WaitForNonStaleResults()).Count();

				var results = PurchaseHistoryView.Create(session, "user1");
				Assert.Equal(2, results.Items.Length);
				Assert.Equal(1, results.Items.Where(x => x.ProductName == "one").Count());
				Assert.Equal(1, results.Items.Where(x => x.ProductName == "two").Count());
			}
		}

		public class PurchaseHistoryView
		{
			public string ShipmentUserId { get; set; }
			public PurchaseHistoryViewItem[] Items { get; set; }

			public static PurchaseHistoryView Create(
				IDocumentSession documentSession,
				string userId)
			{
				return
					new PurchaseHistoryView()
					{
						Items = documentSession.Query<Shipment, PurchaseHistoryIndex>()
									.Customize(x => x.WaitForNonStaleResults(TimeSpan.FromMinutes(5)))
								   .Where(x => x.UserId == userId)
								   .As<PurchaseHistoryViewItem>()
								   .ToArray(),
						ShipmentUserId = userId
					};
			}
		}

		public override void Dispose()
		{
			Store.Dispose();
			base.Dispose();
		}

		public class PurchaseHistoryIndex : AbstractIndexCreationTask
		{
			public override IndexDefinition CreateIndexDefinition()
			{
				return new IndexDefinitionBuilder<Shipment, Shipment>()
				{
					Map = docs => from doc in docs
								  from product in doc.Items
								  select new
								  {
									  UserId = doc.UserId,
									  ProductId = product.ProductId
								  },
					TransformResults = (database, results) =>
						from result in results
						from item in result.Items
						let product = database.Load<Product>(item.ProductId)
						where product != null
						select new
						{
							ProductId = item.ProductId,
							ProductName = product.Name
						}
				}.ToIndexDefinition(Conventions);
			}
		}

		public class PurchaseHistoryViewItem
		{
			public string ProductId { get; set; }
			public string ProductName { get; set; }
		}

		public class Shipment
		{
			public string Id { get; set; }
			public string UserId { get; set; }
			public Address Destination { get; set; }
			public List<ShipmentItem> Items { get; set; }

			public Shipment()
			{
				Items = new List<ShipmentItem>();
			}

			public void AddProduct(Product product)
			{
				this.Items.Add(new ShipmentItem()
				{
					Name = product.Name,
					Price = product.Price,
					ProductId = product.Id
				});
			}
		}

		public class Product
		{
			public string Id { get; set; }
			public string Name { get; set; }
			public string Price { get; set; }
			public string Description { get; set; }
		}

		public class Address
		{
			public string Name { get; set; }
			public string AddressLineOne { get; set; }
			public string AddressLineTwo { get; set; }
			public string AddressLineThree { get; set; }
			public string Town { get; set; }
			public string Region { get; set; }
			public string AreaCode { get; set; }
			public string Country { get; set; }
		}

		public class ShipmentItem
		{
			public string Price { get; set; }
			public string Name { get; set; }
			public string ProductId { get; set; }
		}
	}
}
