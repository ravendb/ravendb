// -----------------------------------------------------------------------
//  <copyright file="MigrationTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Database.Config;
using Raven.SlowTests.Migration.Orders;
using Raven.Tests.Common;

using Xunit;

namespace Raven.SlowTests.Migration
{
    public class MigrationTests : RavenTest
    {
        private Dictionary<string, OrdersByCompany.Result> ordersByCompanyResults;

        private Dictionary<string, ProductSales.Result> productSalesResults;

        protected override void ModifyConfiguration(InMemoryRavenConfiguration configuration)
        {
            configuration.MaxSecondsForTaskToWaitForDatabaseToLoad = 15;

            base.ModifyConfiguration(configuration);
        }

        public MigrationTests()
        {
            FillOrdersByCompany();
            FillProductSales();
        }

        private void FillOrdersByCompany()
        {
            ordersByCompanyResults = new Dictionary<string, OrdersByCompany.Result>();

            using (var file = File.OpenRead("../../Migration/Data/ordersByCompany.csv"))
            using (var reader = new StreamReader(file))
            {
                var line = reader.ReadLine();
                while (line != null)
                {
                    var parts = line.Split(',');
                    ordersByCompanyResults[parts[0]] = new OrdersByCompany.Result
                    {
                        Company = parts[0],
                        Count = (int)(double.Parse(parts[1], CultureInfo.InvariantCulture)),
                        Total = double.Parse(parts[2], CultureInfo.InvariantCulture)
                    };

                    line = reader.ReadLine();
                }
            }
        }

        private void FillProductSales()
        {
            productSalesResults = new Dictionary<string, ProductSales.Result>();

            using (var file = File.OpenRead("../../Migration/Data/productSales.csv"))
            using (var reader = new StreamReader(file))
            {
                var line = reader.ReadLine();
                while (line != null)
                {
                    var parts = line.Split(',');
                    productSalesResults[parts[0]] = new ProductSales.Result
                    {
                        Product = parts[0],
                        Count = (int)(double.Parse(parts[1], CultureInfo.InvariantCulture)),
                        Total = double.Parse(parts[2], CultureInfo.InvariantCulture)
                    };

                    line = reader.ReadLine();
                }
            }
        }

        [Fact]
        public void BasicMigration()
        {
            foreach (var file in Directory.GetFiles("../../Migration/Backups/", "*.zip").Select(x => new FileInfo(x)))
            {
                Console.WriteLine("Processing: " + file.Name);

                using (var store = NewRemoteDocumentStore(runInMemory: false, requestedStorage: file.Name.Contains("esent") ? "esent" : "voron"))
                {
                    store.DefaultDatabase = "Northwind";

                    var extractionDirectory = NewDataPath("Temp");
                    ExtractBackup(file, extractionDirectory);

                    var operation = store
                        .DatabaseCommands
                        .GlobalAdmin
                        .StartRestore(new DatabaseRestoreRequest
                        {
                            BackupLocation = extractionDirectory,
                            DatabaseName = "Northwind"
                        });

                    operation.WaitForCompletion();

                    ValidateBackup(store);

                    // modify docs to force update of indexes on existing data internal data - like mapped results, internal indexes etc, see RavenDB-4677
                    using (var session = store.OpenSession())
                    {
                        var orders = session.Query<Order>().Take(1024).ToList();

                        foreach (var order in orders)
                        {
                            order.OrderedAt = SystemTime.UtcNow;
                        }

                        session.SaveChanges();
                    }

                    ValidateBackup(store);

                    var stats = store.DatabaseCommands.GetStatistics();

                    Assert.True(stats.Errors.Length == 0, $"Indexing errors after migration of : {file.Name}, number of errors: {stats.Errors.Length}");
                }
            }
        }

        private void ValidateBackup(IDocumentStore store)
        {
            WaitForIndexing(store);
            ValidateCounts(store);
            ValidateIndexes(store);
        }

        private void ValidateIndexes(IDocumentStore store)
        {
            WaitForIndexing(store);

            ValidateOrdersByCompany(store);
            ValidateOrdersTotals(store);
            ValidateProductSales(store);
        }

        private void ValidateOrdersByCompany(IDocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                var results = session
                    .Query<OrdersByCompany.Result, OrdersByCompany>()
                    .Take(1024)
                    .ToList();

                Assert.Equal(89, results.Count);

                foreach (var result in results)
                {
                    var expectedResult = ordersByCompanyResults[result.Company];
                    Assert.Equal(expectedResult.Company, result.Company);
                    Assert.Equal(expectedResult.Count, result.Count);
                    Assert.Equal(expectedResult.Total, result.Total);
                }
            }
        }

        private static void ValidateOrdersTotals(IDocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                var count = session
                    .Query<OrdersTotals.Result, OrdersTotals>()
                    .Count();

                Assert.Equal(830, count);
            }
        }

        private void ValidateProductSales(IDocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                var results = session
                    .Query<ProductSales.Result, ProductSales>()
                    .Take(1024)
                    .ToList();

                Assert.Equal(77, results.Count);

                foreach (var result in results)
                {
                    var expectedResult = productSalesResults[result.Product];
                    Assert.Equal(expectedResult.Product, result.Product);
                    Assert.Equal(expectedResult.Count, result.Count);
                    Assert.Equal(expectedResult.Total, result.Total);
                }
            }
        }

        private static void ValidateCounts(IDocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                var statistics = store.DatabaseCommands.GetStatistics();
                Assert.Equal(0, statistics.CountOfAttachments);
                Assert.Equal(4, statistics.Indexes.Length);

                Assert.Equal(8, session.Query<Category>().Count());
                Assert.Equal(91, session.Query<Company>().Count());
                Assert.Equal(9, session.Query<Employee>().Count());
                Assert.Equal(830, session.Query<Order>().Count());
                Assert.Equal(77, session.Query<Product>().Count());
                Assert.Equal(4, session.Query<Region>().Count());
                Assert.Equal(3, session.Query<Shipper>().Count());
                Assert.Equal(29, session.Query<Supplier>().Count());

                Assert.Equal(1051, session.Query<dynamic, RavenDocumentsByEntityName>().Count());
            }
        }

        private static void ExtractBackup(FileSystemInfo file, string extractionDirectory)
        {
            ZipFile.ExtractToDirectory(file.FullName, extractionDirectory);
        }
    }
}
