using System;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Client.Document;
using System.Linq;

namespace Raven.Sample.EventSourcing
{
    class Program
    {
        static void Main()
        {
            var documentStore1 = new DocumentStore { Url = "http://localhost:8080" }.Initialise();

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

            int i = 1;
            foreach (var @event in events)
            {
                documentStore1.DatabaseCommands.Put("events/" + i++, null, JObject.FromObject(@event), new JObject());                
            }

            Console.WriteLine("Wrote {0} events", events.Length);

            Console.ReadLine();

            using(var session = documentStore1.OpenSession())
            {
                var aggregate = session.Query<AggregateHolder>("Aggregates/ShoppingCart")
                    .Where("Id:shoppingcarts/12")
                    .Single();

                var cart = JsonConvert.DeserializeObject<ShoppingCart>(aggregate.Aggregate);

                Console.WriteLine(cart.Total);
            }
        }
    }

    public class AggregateHolder
    {
        public string Id { get; set; }
        public string Aggregate { get; set; }
    }
}
