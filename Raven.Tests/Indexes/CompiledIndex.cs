//-----------------------------------------------------------------------
// <copyright file="CompiledIndex.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.ComponentModel.Composition.Hosting;
using System.Threading;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Client.Embedded;
using Raven.Json.Linq;
using Raven.Database;
using Raven.Database.Config;
using Raven.Tests.Storage;
using Xunit;

namespace Raven.Tests.Indexes
{
	public class CompiledIndex : RavenTest
	{
		private readonly EmbeddableDocumentStore store;
		private readonly DocumentDatabase db;

		public CompiledIndex()
		{
			store =
				NewDocumentStore(new AggregateCatalog
				{Catalogs = {new TypeCatalog(typeof (ShoppingCartEventsToShopingCart), typeof (MapOnlyView))}});
			db = store.DocumentDatabase;
			db.SpinBackgroundWorkers();
		}

		public override void Dispose()
		{
			store.Dispose();
			base.Dispose();
		}

		[Fact]
		public void CanGetDataFromCompiledIndex()
		{
			db.Put("events/1", null, RavenJObject.FromObject(new
			{
				For = "ShoppingCart",
				Type = "Create",
				Timestamp = SystemTime.Now,
				ShoppingCartId = "shoppingcarts/12",
				CustomerId = "users/ayende",
				CustomerName = "Ayende Rahien"
			}), new RavenJObject(), null);

			QueryResult queryResult;
			do
			{
				queryResult = db.Query("Compiled/View", new IndexQuery
				{
					Query = "CustomerId:users/ayende"
				});
				if (queryResult.IsStale)
					Thread.Sleep(100);
			} while (queryResult.IsStale);

			Assert.Equal(1, queryResult.TotalResults);

		}

		[Fact]
		public void CompileIndexWillTurnEventsToAggregate()
		{
			var events = new object[]
			{
				new
				{
					For = "ShoppingCart",
					Type = "Create",
					Timestamp = SystemTime.Now,
					ShoppingCartId = "shoppingcarts/12",
					CustomerId = "users/ayende",
					CustomerName = "Ayende Rahien"
				},
				new
				{
					For = "ShoppingCart",
					Type = "Add",
					Timestamp = SystemTime.Now,
					ShoppingCartId = "shoppingcarts/12",
					ProductId = "products/8123",
					ProductName = "Fish & Chips",
					Price = 8.5m
				},
				new
				{
					For = "ShoppingCart",
					Type = "Add",
					Timestamp = SystemTime.Now,
					ShoppingCartId = "shoppingcarts/12",
					ProductId = "products/3214",
					ProductName = "Guinness",
					Price = 2.1m
				},
				new
				{
					For = "ShoppingCart",
					Type = "Remove",
					Timestamp = SystemTime.Now,
					ShoppingCartId = "shoppingcarts/12",
					ProductId = "products/8123",
					ProductName = "Fish & Chips",
					Price = 8.5m
				},
				new
				{
					For = "ShoppingCart",
					Type = "Add",
					Timestamp = SystemTime.Now,
					ShoppingCartId = "shoppingcarts/12",
					ProductId = "products/8121",
					ProductName = "Beef Pie",
					Price = 9.0m
				},
			};
			for (int i = 0; i < events.Length; i++)
			{
				db.Put("events/" + (i + 1), null, RavenJObject.FromObject(events[i]), new RavenJObject(), null);
			}

			QueryResult queryResult = null;
			for (int i = 0; i < 500; i++)
			{
				queryResult = db.Query("Aggregates/ShoppingCart", new IndexQuery());
				if (queryResult.IsStale)
					Thread.Sleep(100);
				else
					break;
			}

			Assert.Empty(db.Statistics.Errors);

			Assert.Equal(1, queryResult.Results.Count);

			Assert.Equal("shoppingcarts/12", queryResult.Results[0].Value<string>("ShoppingCartId"));
			var ravenJObject = queryResult.Results[0].Value<RavenJObject>("Aggregate");
			var cart = ravenJObject.JsonDeserialization<ShoppingCartEventsToShopingCart.ShoppingCart>();
			Assert.Equal(2, cart.ItemsCount);
		}
	}
}