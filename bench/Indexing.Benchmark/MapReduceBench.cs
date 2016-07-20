using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Indexing.Benchmark.Entities;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Client.Indexing;

namespace Indexing.Benchmark
{
    public class MapReduceBench : IDisposable
    {
        private readonly Random _random;
        private readonly IDocumentStore _store;

        public MapReduceBench(string url = "http://localhost:8080", int? seed = null)
        {
            _store = new DocumentStore
            {
                Url = url,
                DefaultDatabase = "map-reduce-benchmark"
            }.Initialize(ensureDatabaseExists: true);

            _random = new Random(seed ?? Environment.TickCount);
        }

        public void Execute()
        {
            var numberOfDocuments = 10000;

            Console.WriteLine($"Inserting {numberOfDocuments} orders ... ");

            using (var bulk = _store.BulkInsert())
            {
                for (int i = 0; i < numberOfDocuments; i++)
                {
                    bulk.StoreAsync(new Order
                    {
                        Company = $"companies/{_random.Next(0, numberOfDocuments / 100)}",
                        Employee = $"employees/{_random.Next(0, numberOfDocuments / 100)}",
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

                    if (i%1000 == 0)
                    {
                        Console.WriteLine($"Inserted {i} documents");
                    }
                }
            }

            Console.WriteLine("done");

            var ordersByCompany = new Orders_ByCompany();

            Console.WriteLine($"Inserting {ordersByCompany.IndexName} index");

            ordersByCompany.Execute(_store);

            Console.WriteLine("waiting for results ...");

            var sw = Stopwatch.StartNew();

            var isStale = true;
            TimeSpan lastCheck = TimeSpan.Zero;

            while (isStale)
            {
                Thread.Sleep(1000);

                isStale = _store.DatabaseCommands.GetStatistics().StaleIndexes.Length != 0;

                if (sw.Elapsed - lastCheck > TimeSpan.FromSeconds(1))
                {
                    lastCheck = sw.Elapsed;

                    //var stats = _store.DatabaseCommands.GetIndexStatistics(ordersByCompany.IndexName);

                    //Console.WriteLine($"{nameof(stats.MapAttempts)}: {stats.MapAttempts} of {numberOfDocuments}");
                    //Console.WriteLine($"{nameof(stats.ReduceAttempts)}: {stats.ReduceAttempts}");
                }
            }

            sw.Stop();

            Console.WriteLine($"It took {sw.Elapsed} to index {numberOfDocuments} orders");
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

    public class Orders_ByCompany : AbstractIndexCreationTask
    {
        public class Result
        {
            public string Company { get; set; }

            public int Count { get; set; }

            public double Total { get; set; }
        }

        public override string IndexName
        {
            get
            {
                return "Orders/ByCompany";
            }
        }

        public override IndexDefinition CreateIndexDefinition()
        {
            return new IndexDefinition
            {
                Maps = { @"from order in docs.Orders
                            from line in order.Lines
                            select
                            new
                            {
                                order.Company,
                                Count = 1,
                                Total = line.PricePerUnit
                            }" },
                Reduce = @"from result in results
group result by result.Company into g
select new
{
    Company = g.Key,
    Count = g.Sum(x=> x.Count),
    Total = g.Sum(x=> x.Total)
}"
            };
        }
    }

    //public class Orders_ByCompany : AbstractIndexCreationTask<Order, Orders_ByCompany.Result>
    //{
    //    public class Result
    //    {
    //        public string Company { get; set; }

    //        public int Count { get; set; }

    //        public double Total { get; set; }
    //    }

    //    public Orders_ByCompany()
    //    {
    //        // currently we don't have 
    //        //Map = orders => from order in orders
    //        //                select
    //        //                new
    //        //                {
    //        //                    order.Company,
    //        //                    Count = 1,
    //        //                    Total = order.Lines.Sum(l => (l.Quantity * l.PricePerUnit) * (1 - l.Discount))
    //        //                };

    //        Map = orders => from order in orders
    //                        from line in order.Lines
    //                        select
    //                        new
    //                        {
    //                            order.Company,
    //                            Count = 1,
    //                            Total = line.PricePerUnit
    //                        };

    //        Reduce = results => from result in results
    //                            group result by result.Company
    //            into g
    //                            select new
    //                            {
    //                                Company = g.Key,
    //                                Count = g.Sum(x => x.Count),
    //                                Total = g.Sum(x => x.Total)
    //                            };
    //    }
    //}
}