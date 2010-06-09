using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.ComponentModel.Composition.Hosting;
using System.Linq;
using System.Threading;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Data;
using Raven.Database.Indexing;
using Raven.Database.Linq;
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
            db.Put("events/1", null, JObject.FromObject(new
            {
                For = "ShoppingCart",
                Type = "Create",
                Timestamp = DateTime.Now,
                ShoppingCartId = "shoppingcarts/12",
                CustomerId = "users/ayende",
                CustomerName = "Ayende Rahien"
            }), new JObject(), null);

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
                db.Put("events/" + (i + 1), null, JObject.FromObject(events[i]), new JObject(), null);
            }

            QueryResult queryResult;
            do
            {
                queryResult = db.Query("Aggregates/ShoppingCart", new IndexQuery());
                if (queryResult.IsStale)
                    Thread.Sleep(100);
            } while (queryResult.IsStale);

            Assert.Equal(1, queryResult.Results.Length);

			Assert.Equal("shoppingcarts/12", queryResult.Results[0].Value<string>("ShoppingCartId"));
        	var cart =
        		queryResult.Results[0].Value<JObject>("Aggregate").JsonDeserialization
        			<ShoppingCartEventsToShopingCart.ShoppingCart>();
            Assert.Equal(2, cart.Items.Count);
        }
    }

    [DisplayName("Compiled/View")]
    public class MapOnlyView : AbstractViewGenerator
    {
        public MapOnlyView()
        {
            MapDefinition = source => from doc in source
                                      select doc;
        }
    }

    [DisplayName("Aggregates/ShoppingCart")]
    public class ShoppingCartEventsToShopingCart : AbstractViewGenerator
    {
        public ShoppingCartEventsToShopingCart()
        {
            MapDefinition = docs => docs.Where(document => document.For == "ShoppingCart");
            GroupByExtraction = source => source.ShoppingCartId;
            ReduceDefinition = Reduce;

            Indexes.Add("Id", FieldIndexing.NotAnalyzed);
            Indexes.Add("Aggregate", FieldIndexing.No);
        }

        private static IEnumerable<object> Reduce(IEnumerable<dynamic> source)
        {
            foreach (var events in source
                .GroupBy(@event => @event.ShoppingCartId))
            {
                var cart = new ShoppingCart { Id = events.Key };
                foreach (var @event in events.OrderBy(x => x.Timestamp))
                {
                    switch ((string)@event.Type)
                    {
                        case "Create":
                            cart.Customer = new ShoppingCartCustomer
                            {
                                Id = @event.CustomerId,
                                Name = @event.CustomerName
                            };
                            break;
                        case "Add":
                            cart.AddToCart(@event.ProductId, @event.ProductName, (decimal)@event.Price);
                            break;
                        case "Remove":
                            cart.RemoveFromCart(@event.ProductId);
                            break;
                    }
                }
                yield return new
                {
					ShoppingCartId = cart.Id,
                    Aggregate = JObject.FromObject(cart)
                };
            }
        }

        public class ShoppingCart
        {
            public string Id { get; set; }
            public ShoppingCartCustomer Customer { get; set; }
            public List<ShoppingCartItem> Items { get; set; }
            public decimal Total { get { return Items.Sum(x => x.Product.Price * x.Quantity); } }

            public ShoppingCart()
            {
                Items = new List<ShoppingCartItem>();
            }

            public void AddToCart(string productId, string productName, decimal price)
            {
                var item = Items.FirstOrDefault(x => x.Product.Id == productId);
                if (item != null)
                {
                    item.Quantity++;
                    return;
                }
                Items.Add(new ShoppingCartItem
                {
                    Product = new ShoppingCartItemProduct
                    {
                        Id = productId,
                        Name = productName,
                        Price = price
                    },
                    Quantity = 1
                });
            }

            public void RemoveFromCart(string productId)
            {
                var shoppingCartItem = Items.FirstOrDefault(x => x.Product.Id == productId);
                if (shoppingCartItem == null)
                    return;
                Items.Remove(shoppingCartItem);
            }
        }
        public class ShoppingCartCustomer
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        public class ShoppingCartItem
        {
            public ShoppingCartItemProduct Product { get; set; }
            public int Quantity { get; set; }

        }

        public class ShoppingCartItemProduct
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public decimal Price { get; set; }
        }
    }
}