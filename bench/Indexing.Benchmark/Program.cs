using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Indexing.Benchmark.Entities;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Extensions;
using Raven.Client.Server;
using Raven.Client.Server.Operations;

#if v35
using Raven.NewClient.Abstractions.Data;
#endif

namespace Indexing.Benchmark
{
    public class Program : IDisposable
    {
        private const string DbName = "indexing-benchmark";

        private const int NumberOfOrderDocuments = 100000;
        private const int NumberOfCompanyDocuments = 50000;
        private const int NumberOfEmployeeDocuments = 50000;

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
                Urls = new [] { url },
                Database = DbName
            }.Initialize();

            if (_store.Admin.Server.Send(new GetDatabaseNamesOperation(0, 100)).Contains(DbName))
            {
                _store.Admin.Send(new GetStatisticsOperation()); // give some time for database to load
                _store.Admin.Server.Send(new DeleteDatabaseOperation(DbName, hardDelete: true));
            }

            var doc = MultiDatabase.CreateDatabaseDocument(DbName);

            _store.Admin.Server.Send(new CreateDatabaseOperation(doc));
        }

        public void Execute()
        {
            InsertDocs();

            Console.WriteLine("Starting map benchmark");
            var mapIndexesBench = new MapIndexesBench(_store, NumberOfOrderDocuments);
            mapIndexesBench.Execute();
            Console.WriteLine($"{Environment.NewLine}-----------------------------");

            Console.WriteLine("Starting map-reduce benchmark");
            var mapReduceIndexesBench = new MapReduceIndexesBench(_store, NumberOfOrderDocuments);
            mapReduceIndexesBench.Execute();
            Console.WriteLine($"{Environment.NewLine}-----------------------------");

            _store.Admin.Send(new StopIndexingOperation());

            Console.WriteLine($"Indexing stopped. Number of indexes in the database: {_store.Admin.Send(new GetStatisticsOperation()).Indexes.Length}");

            var staleIndexes = new HashSet<string>();

            foreach (var indexName in _store.Admin.Send(new GetIndexNamesOperation(0, 1024)))
            {
                _store.Admin.Send(new ResetIndexOperation(indexName));
                staleIndexes.Add(indexName);
            }

            Console.WriteLine("All indexes have been reset");

            _store.Admin.Send(new StartIndexingOperation());

            var sw = Stopwatch.StartNew();

            Console.WriteLine("Indexing is working again. Waiting for non stale results");

            WaitForNonStaleIndexes(staleIndexes, sw);

            _store.Admin.Send(new StopIndexingOperation());

            Console.WriteLine($"Indexing stopped. Number of indexes in the database: {_store.Admin.Send(new GetStatisticsOperation()).Indexes.Length}");

            staleIndexes = new HashSet<string>();

            foreach (var indexName in _store.Admin.Send(new GetIndexNamesOperation(0, 1024)))
            {
                _store.Admin.Send(new ResetIndexOperation(indexName));
                staleIndexes.Add(indexName);
            }

            Console.WriteLine("All indexes have been reset");

            var numberOfAdditionaMapIndexes = 10;
            var numberOfAdditionalMapReduceIndexes = 5;

            Console.WriteLine($"Putting additional indexes: {numberOfAdditionaMapIndexes} map and {numberOfAdditionalMapReduceIndexes} map-reduce");

            for (int i = 0; i < numberOfAdditionaMapIndexes / 2; i++)
            {
                var employeesByNameAndAddress = new MapIndexesBench.Employees_ByNameAndAddress(i);
                employeesByNameAndAddress.Execute(_store);

                staleIndexes.Add(employeesByNameAndAddress.IndexName);

                var companiesByNameAndEmail = new MapIndexesBench.Companies_ByNameAndEmail(i);
                companiesByNameAndEmail.Execute(_store);

                staleIndexes.Add(companiesByNameAndEmail.IndexName);
            }

            for (int i = 0; i < numberOfAdditionalMapReduceIndexes; i++)
            {
                var employeesGroupByCountry = new MapReduceIndexesBench.Employees_GroupByCountry(i);
                employeesGroupByCountry.Execute(_store);

                staleIndexes.Add(employeesGroupByCountry.IndexName);
            }

            _store.Admin.Send(new StartIndexingOperation());

            sw = Stopwatch.StartNew();

            Console.WriteLine($"Indexing is working again. Number of indexes in the database: {_store.Admin.Send(new GetStatisticsOperation()).Indexes.Length}. Waiting for non stale results");

            WaitForNonStaleIndexes(staleIndexes, sw);
        }

        private void WaitForNonStaleIndexes(HashSet<string> staleIndexes, Stopwatch sw)
        {
            do
            {
                var stats = _store.Admin.Send(new GetStatisticsOperation());

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
