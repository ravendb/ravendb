//-----------------------------------------------------------------------
// <copyright file="CompiledIndex.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.ComponentModel.Composition.Hosting;
using System.Threading;
using Newtonsoft.Json;
using Raven.Json.Linq;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Data;
using Raven.Tests.Storage;
using Xunit;
using Raven.Database.Json;

namespace Raven.Tests.Indexes
{
    public class CompiledIndex : AbstractDocumentStorageTest
    {
        private readonly DocumentDatabase db;

        public CompiledIndex()
        {
            db = new DocumentDatabase(new RavenConfiguration
            {
                DataDirectory = "raven.db.test.esent",
                RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true,
                Catalog = { Catalogs = { new TypeCatalog(typeof(ShoppingCartEventsToShopingCart), typeof(MapOnlyView)) } }
            });
            db.SpinBackgroundWorkers();
        }

        #region IDisposable Members

        public override void Dispose()
        {
            db.Dispose();
            base.Dispose();
        }

        #endregion

        [Fact]
        public void CanGetDataFromCompiledIndex()
        {
            db.Put("events/1", null, RavenJObject.FromObject(new
            {
                For = "ShoppingCart",
                Type = "Create",
                Timestamp = DateTime.Now,
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
                    Timestamp = DateTime.Now,
                    ShoppingCartId = "shoppingcarts/12",
                    CustomerId = "users/ayende",
                    CustomerName = "Ayende Rahien"
                },
                new
                {
                    For = "ShoppingCart",
                    Type = "Add",
                    Timestamp = DateTime.Now,
                    ShoppingCartId = "shoppingcarts/12",
                    ProductId = "products/8123",
                    ProductName = "Fish & Chips",
                    Price = 8.5m
                },
                new
                {
                    For = "ShoppingCart",
                    Type = "Add",
                    Timestamp = DateTime.Now,
                    ShoppingCartId = "shoppingcarts/12",
                    ProductId = "products/3214",
                    ProductName = "Guinness",
                    Price = 2.1m
                },
                new
                {
                    For = "ShoppingCart",
                    Type = "Remove",
                    Timestamp = DateTime.Now,
                    ShoppingCartId = "shoppingcarts/12",
                    ProductId = "products/8123"
                },
                new
                {
                    For = "ShoppingCart",
                    Type = "Add",
                    Timestamp = DateTime.Now,
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

            QueryResult queryResult;
            do
            {
                queryResult = db.Query("Aggregates/ShoppingCart", new IndexQuery());
                if (queryResult.IsStale)
                    Thread.Sleep(100);
            } while (queryResult.IsStale);

			Assert.Equal(1, queryResult.Results.Count);

			Assert.Equal("shoppingcarts/12", queryResult.Results[0].Value<string>("ShoppingCartId"));
        	var cart =
        		queryResult.Results[0].Value<RavenJObject>("Aggregate").JsonDeserialization
        			<ShoppingCartEventsToShopingCart.ShoppingCart>();
            Assert.Equal(2, cart.Items.Count);
        }
    }
}
