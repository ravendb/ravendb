using System;
using System.Collections.Generic;
using System.Diagnostics;
using Indexing.Benchmark.Entities;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Extensions;
using System.Linq;
using System.Threading;
#if v35
using Raven.Abstractions.Data;
#else
using Raven.Client.Data;
#endif

namespace Indexing.Benchmark
{
    public class Program : IDisposable
    {
        private const string DbName = "indexing-benchmark";
        private const int NumberOfOrderDocuments = 100000;
        private const int NumberOfCompanyDocuments = 1000;
        private const int NumberOfEmployeeDocuments = 1000;

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
            }.Initialize();

            while (_store.DatabaseCommands.GlobalAdmin.GetDatabaseNames(100).Contains(DbName))
            {
                try
                {
                    _store.DatabaseCommands.GlobalAdmin.DeleteDatabase(DbName, true);
                }
                catch (Exception e)
                {
                    Console.WriteLine($"Failed to delete database '{DbName}'. Exception: {e}");

                    var timeToWait = TimeSpan.FromSeconds(2);

                    Console.WriteLine($"waiting {timeToWait} and trying again ... ");
                    Thread.Sleep(timeToWait);
                }
            }

            var doc = MultiDatabase.CreateDatabaseDocument(DbName);

            _store.DatabaseCommands.GlobalAdmin.CreateDatabase(doc);
        }

        public void Execute()
        {
            InsertOrders();

            InsertCompanies();

            InsertEmployees();

            Console.WriteLine("Starting map benchmark");
            var mapIndexesBench = new MapIndexesBench(_store, NumberOfOrderDocuments);
            mapIndexesBench.Execute();
            Console.WriteLine($"{Environment.NewLine}-----------------------------");

            Console.WriteLine("Starting map-reduce benchmark");
            var mapReduceIndexesBench = new MapReduceIndexesBench(_store, NumberOfOrderDocuments);
            mapReduceIndexesBench.Execute();
            Console.WriteLine($"{Environment.NewLine}-----------------------------");

            _store.DatabaseCommands.Admin.StopIndexing();

            Console.WriteLine("Indexing stopped");

            var staleIndexes = new HashSet<string>();

            foreach (var indexName in _store.DatabaseCommands.GetIndexNames(0, 1024))
            {
                _store.DatabaseCommands.ResetIndex(indexName);
                staleIndexes.Add(indexName);
            }

            Console.WriteLine("All indexes reset");

            _store.DatabaseCommands.Admin.StartIndexing();

            var sw = Stopwatch.StartNew();

            Console.WriteLine("Indexing is working again. Waiting for non stale results");

            DatabaseStatistics stats;
            do
            {
                stats = _store.DatabaseCommands.GetStatistics();

                if (stats.StaleIndexes.Length != staleIndexes.Count)
                {
                    var newNonStaleIndexes = staleIndexes.Except(stats.StaleIndexes).ToList();

                    foreach (var name in newNonStaleIndexes)
                    {
                        Console.WriteLine($"Index {name} became non stale after {sw.Elapsed}");

                        staleIndexes.Remove(name);
                    }
                }

                Thread.Sleep(100);
            } while (staleIndexes.Count > 0);
        }

        private void InsertOrders()
        {
            Console.WriteLine($"Inserting {NumberOfOrderDocuments:#,#} orders ... ");

            using (var bulk = _store.BulkInsert())
            using (var progress = new ProgressBar())
            {
                for (int i = 0; i < NumberOfOrderDocuments; i++)
                {
                    var order = new Order
                    {
                        Company = $"companies/{_random.Next(1, NumberOfCompanyDocuments)}",
                        Employee = $"employees/{_random.Next(1, NumberOfEmployeeDocuments)}",
                        Lines = CreateOrderLines(_random.Next(0, 15)),
                        Freight = _random.Next(),
                        OrderedAt = DateTime.Now,
                        RequireAt = DateTime.Now.AddDays(_random.Next(1, 30)),
                        ShippedAt = DateTime.Now,
                        ShipTo = new Address
                        {
                            Country = $"Country{_random.Next(0, 10)}",
                            City = $"City{_random.Next(0, 100)}",
                            Street = $"Street{_random.Next(0, 1000)}",
                            ZipCode = _random.Next(0, 9999)
                        },
                        ShipVia = $"shippers/{_random.Next(0, 10)}"
                    };

#if v35
                    bulk.Store(order);
#else
                    bulk.StoreAsync(order).Wait();
#endif
                    progress.Report((double)i/NumberOfOrderDocuments);
                }

                progress.Report(100);
            }

            Console.WriteLine($"{Environment.NewLine}-----------------------------");
        }

        private void InsertCompanies()
        {
            Console.WriteLine($"Inserting {NumberOfCompanyDocuments:#,#} companies ... ");

            using (var bulk = _store.BulkInsert())
            using (var progress = new ProgressBar())
            {
                for (int i = 0; i < NumberOfCompanyDocuments; i++)
                {
                    var company = new Company()
                    {
                        Name = $"Company-{i}",
                        Desc = "Lorem ipsum ble ble ble",
                        Contacts = new List<Contact>()
                        {
                            new Contact(), new Contact(), new Contact()
                        },
                        Address1 = "Address1",
                        Address2 = "Address2",
                        Address3 = "Address3",
                        Email = $"company-{i}@gmail.com",
                    };

#if v35
                    bulk.Store(company);
#else
                    bulk.StoreAsync(company).Wait();
#endif
                    progress.Report((double)i / NumberOfCompanyDocuments);
                }

                progress.Report(100);
            }

            Console.WriteLine($"{Environment.NewLine}-----------------------------");
        }

        private void InsertEmployees()
        {
            Console.WriteLine($"Inserting {NumberOfEmployeeDocuments:#,#} employees ... ");

            using (var bulk = _store.BulkInsert())
            using (var progress = new ProgressBar())
            {
                for (int i = 0; i < NumberOfCompanyDocuments; i++)
                {
                    var employee = new Employee()
                    {
                        FirstName = $"FirstName-{i}",
                        LastName = $"LastName-{i}",
                    };

#if v35
                    bulk.Store(employee);
#else
                    bulk.StoreAsync(employee).Wait();
#endif
                    progress.Report((double)i / NumberOfCompanyDocuments);
                }

                progress.Report(100);
            }

            Console.WriteLine($"{Environment.NewLine}-----------------------------");
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
