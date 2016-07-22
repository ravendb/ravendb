using System;
using System.Collections.Generic;
using Indexing.Benchmark.Entities;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Extensions;
using System.Linq;

namespace Indexing.Benchmark
{
    public class Program : IDisposable
    {
        private const string DbName = "indexing-benchmark";
        private const int NumberOfOrderDocuments = 10000;

        private readonly IDocumentStore _store;
        private readonly Random _random;

        public static void Main(string[] args)
        {
            using (var bench = new Program(seed: 1))
            {
                bench.Execute();
            }
        }

        public Program(string url = "http://localhost:8080", int? seed = null)
        {
            _random = new Random(seed ?? Environment.TickCount);

            _store = new DocumentStore
            {
                Url = url,
                DefaultDatabase = DbName
            }.Initialize(ensureDatabaseExists: true); // TODO - suport for ensureDatabaseExists has been removed - not sure why

            if (_store.DatabaseCommands.GlobalAdmin.GetDatabaseNames(100).Contains(DbName))
            {
                _store.DatabaseCommands.GlobalAdmin.DeleteDatabase(DbName, true);
            }

            var doc = MultiDatabase.CreateDatabaseDocument(DbName);

            _store.DatabaseCommands.GlobalAdmin.CreateDatabase(doc);
        }

        public void Execute()
        {
            Console.WriteLine($"Inserting {NumberOfOrderDocuments} orders ... ");
            InsertOrders();
            Console.WriteLine("done");

            Console.WriteLine("Starting map benchmark");
            new MapIndexesBench(_store).Execute();
            Console.WriteLine("done");

            //Console.WriteLine("Press ENTER to move on");
            //Console.ReadLine();

            Console.WriteLine("Starting map-reduce benchmark");
            new MapReduceIndexesBench(_store).Execute();
            Console.WriteLine("done");
        }

        private void InsertOrders()
        {
            using (var bulk = _store.BulkInsert())
            {
                for (int i = 0; i < NumberOfOrderDocuments; i++)
                {
#if v35
                        bulk.Store(new Order
                        {
                            Company = $"companies/{_random.Next(0, NumberOfOrderDocuments / 100)}",
                            Employee = $"employees/{_random.Next(0, NumberOfOrderDocuments / 100)}",
                            Lines = CreateOrderLines(_random.Next(0, 100)),
                            Freight = _random.Next(),
                            OrderedAt = DateTime.Now,
                            RequireAt = DateTime.Now.AddDays(_random.Next(1, 30)),
                            ShippedAt = DateTime.Now,
                            ShipTo = new Address()
                            {
                                City = "City",
                                Country = "Country",
                                Street = "Street",
                                ZipCode = 12345
                            },
                            ShipVia = $"shippers/{_random.Next(0, 10)}"
                        });
#else
                    bulk.StoreAsync(new Order
                    {
                        Company = $"companies/{_random.Next(0, NumberOfOrderDocuments / 100)}",
                        Employee = $"employees/{_random.Next(0, NumberOfOrderDocuments / 100)}",
                        Lines = CreateOrderLines(_random.Next(0, 100)),
                        Freight = _random.Next(),
                        OrderedAt = DateTime.Now,
                        RequireAt = DateTime.Now.AddDays(_random.Next(1, 30)),
                        ShippedAt = DateTime.Now,
                        ShipTo = new Address()
                        {
                            City = "City",
                            Country = "Country",
                            Street = "Street",
                            ZipCode = 12345
                        },
                        ShipVia = $"shippers/{_random.Next(0, 10)}"
                    }).Wait();
#endif

                    if (i % 1000 == 0)
                    {
                        Console.WriteLine($"Inserted {i} docs");
                    }
                }

                Console.WriteLine($"Inserted {NumberOfOrderDocuments} docs");
            }
        }

        private List<OrderLine> CreateOrderLines(int count)
        {
            var lines = new List<OrderLine>(count);

            for (int i = 0; i < count; i++)
            {
                lines.Add(new OrderLine()
                {
                    Discount = _random.Next(0, 1),
                    PricePerUnit = _random.Next(1, 999),
                    Product = $"products/{_random.Next(1, 9999)}",
                    ProductName = "ProductName"
                });
            }

            return lines;
        }

        public void Dispose()
        {
            _store.Dispose();
        }
    }
}
