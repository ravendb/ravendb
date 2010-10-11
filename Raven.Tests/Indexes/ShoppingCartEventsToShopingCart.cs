using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using Newtonsoft.Json.Linq;
using Raven.Database.Indexing;
using Raven.Database.Linq;

namespace Raven.Tests.Indexes
{
    [DisplayName("Aggregates/ShoppingCart")]
    public class ShoppingCartEventsToShopingCart : AbstractViewGenerator
    {
        public ShoppingCartEventsToShopingCart()
        {
            MapDefinition = docs => docs.Where<object>(document => document.For == "ShoppingCart");
            GroupByExtraction = source => source.ShoppingCartId;
            ReduceDefinition = Reduce;

            Indexes.Add("Id", FieldIndexing.NotAnalyzed);
            Indexes.Add("Aggregate", FieldIndexing.No);
        }

        private static IEnumerable<object> Reduce(IEnumerable<object> source)
        {
            foreach (var events in source
                .GroupBy<object, object>(@event => @event.ShoppingCartId))
            {
                var cart = new ShoppingCart { Id = events.Key };
                foreach (var @event in events.OrderBy<object, object>(x => x.Timestamp))
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