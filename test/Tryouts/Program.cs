using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Json;
using Raven.Client.Document;
using Raven.Client.Extensions;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using FastTests;
using Raven.Client;
using StressTests;
using Voron;

namespace Tryouts
{
    public class Program
    {
        static void Main(string[] args)
        {
            using (var store = new DocumentStore
            {
                Url = "http://localhost:8080",
                DefaultDatabase = "test"
            }.Initialize())
            {
                var max = 50000;

                InsertDocs(store, max);
            }
        }

        private static void InsertDocs(IDocumentStore store, int max)
        {
            var random = new Random(1);

            using (var bulk = store.BulkInsert())
            {
                for (int i = 0; i < max; i++)
                {
                    var order = new Order
                    {
                        Id = "orders/" + i,
                        Company = "companies/1",
                        Employee = "employees/1",
                        Lines = CreateOrderLines(random.Next(0, 15), random),
                        Freight = random.Next(),
                        OrderedAt = DateTime.Now,
                        RequireAt = DateTime.Now.AddDays(random.Next(1, 30)),
                        ShippedAt = DateTime.Now,
                        ShipTo = new Address
                        {
                            Country = $"Country{random.Next(0, 10)}",
                            City = $"City{random.Next(0, 100)}",
                            Street = $"Street{random.Next(0, 1000)}",
                            ZipCode = random.Next(0, 9999)
                        },
                        ShipVia = "shippers/1"
                    };

                    bulk.StoreAsync(order).Wait();
                }
            }
        }

        private static List<OrderLine> CreateOrderLines(int count, Random random)
        {
            var lines = new List<OrderLine>(count);

            for (int i = 0; i < count; i++)
            {
                var orderLine = new OrderLine()
                {
                    Discount = random.Next(0, 1),
                    PricePerUnit = random.Next(1, 999),
                    Product = $"products/{random.Next(1, 9999)}",
                    ProductName = "ProductName"
                };

                lines.Add(orderLine);
            }

            return lines;
        }

        private class Order
        {
            public string Id { get; set; }
            public string Company { get; set; }
            public string Employee { get; set; }
            public DateTime OrderedAt { get; set; }
            public DateTime RequireAt { get; set; }
            public DateTime? ShippedAt { get; set; }
            public Address ShipTo { get; set; }
            public string ShipVia { get; set; }
            public decimal Freight { get; set; }
            public List<OrderLine> Lines { get; set; }
        }

        private class OrderLine
        {
            public string Product { get; set; }
            public string ProductName { get; set; }
            public decimal PricePerUnit { get; set; }
            public int Quantity { get; set; }
            public decimal Discount { get; set; }
        }

        private class Address
        {
            public string Id { get; set; }
            public string Country { get; set; }
            public string City { get; set; }
            public string Street { get; set; }
            public int ZipCode { get; set; }
        }
    }

}

