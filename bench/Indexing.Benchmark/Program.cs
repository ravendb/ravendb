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

        private const int NumberOfOrderDocuments = 200000;
        private const int NumberOfCompanyDocuments = 50000;
        private const int NumberOfEmployeeDocuments = 50000;

        private readonly IDocumentStore _store;
        private Random _random;

        public static void Main(string[] args)
        {
            using (var bench = new Program(seed: 1))
            {
                bench.Execute();
            }
        }

        public Program(string url = "http://localhost:8080", int? seed = null)
        {
            _random = new Random(1);

            _store = new DocumentStore
            {
                Url = url,
                DefaultDatabase = DbName
            }.Initialize();

            if (_store.DatabaseCommands.GlobalAdmin.GetDatabaseNames(100).Contains(DbName))
            {
                _store.DatabaseCommands.GetStatistics(); // give some time for database to load
                _store.DatabaseCommands.GlobalAdmin.DeleteDatabase(DbName, true);
            }

            var doc = MultiDatabase.CreateDatabaseDocument(DbName);

            _store.DatabaseCommands.GlobalAdmin.CreateDatabase(doc);
        }

        public void Execute()
        {
            InsertDocs();

            _random = new Random(1);

            InsertDocs();

            return;
        }

        private void WaitForNonStaleIndexes(HashSet<string> staleIndexes, Stopwatch sw)
        {
            do
            {
                var stats = _store.DatabaseCommands.GetStatistics();

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

        private void InsertDocs()
        {
            Console.WriteLine($"Inserting {NumberOfOrderDocuments:#,#} orders ... ");
            Console.WriteLine($"Inserting {NumberOfCompanyDocuments:#,#} companies ... ");
            Console.WriteLine($"Inserting {NumberOfEmployeeDocuments:#,#} employees ... ");

            int numberOfIterations = Math.Max(NumberOfOrderDocuments,
                Math.Max(NumberOfEmployeeDocuments, NumberOfCompanyDocuments));

            using (var bulk = _store.BulkInsert())
            using (var progress = new ProgressBar())
            {
                for (int i = 0; i < numberOfIterations; i++)
                {
                    if (i < NumberOfOrderDocuments)
                    {
                        var order = new Order
                        {
                            Id = "orders/" + i,
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
                    }

                    if (i < NumberOfCompanyDocuments)
                    {
                        var company = new Company()
                        {
                            Id = "companies/" + i,
                            Name = $"Company-{i}",
                            Desc = "Lorem ipsum ble ble ble",
                            Contacts = new List<Contact>()
                            {
                                new Contact(),
                                new Contact(),
                                new Contact()
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
                    }

                    if (i < NumberOfEmployeeDocuments)
                    {
                        var employee = new Employee()
                        {
                            Id = "employees/" + i,
                            FirstName = $"FirstName-{i % 123}",
                            LastName = $"LastName-{i % 456}",
                            Address = new Address()
                            {
                                Country = i % 2 == 0 ? "PL" : "IL",
                                City = $"City-{i % 77}",
                                Street = $"Street-{i % 199}",
                                ZipCode = i % 999
                            }
                        };

#if v35
                    bulk.Store(employee);
#else
                        bulk.StoreAsync(employee).Wait();
#endif
                    }

                    progress.Report((double)i / numberOfIterations);
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
