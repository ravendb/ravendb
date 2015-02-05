// -----------------------------------------------------------------------
//  <copyright file="MigrationTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Xml.Linq;

using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Database.Extensions;
using Raven.Tests.Common;
using Raven.Tests.Helpers;
using Raven.Tests.Migration.Indexes;
using Raven.Tests.Migration.Utils;
using Raven.Tests.Migration.Utils.Orders;

using Xunit;

namespace Raven.Tests.Migration
{
	public class MigrationTests : RavenTest
	{
		private const int Port = 8075;

		private const string DatabaseName = "Northwind";

		private const string PackageName = "RavenDB.Server";

		private readonly List<string> packageNames;

		public MigrationTests()
		{
			packageNames = ParseServers().ToList();
		}

		private static IEnumerable<string> ParseServers()
		{
			var document = XDocument.Load("../../packages.config");
			if (document.Root == null)
				yield break;

			var nodes = document.Root.Descendants().ToList();
			foreach (var node in nodes)
			{
				var attributes = node.Attributes().ToList();
				var id = attributes.First(x => x.Name == "id").Value;
				if (string.Equals(id, PackageName, StringComparison.OrdinalIgnoreCase) == false)
					continue;

				yield return PackageName + "." + attributes.First(x => x.Name == "version").Value;
			}
		}

		[Fact]
		public void LargeMigration()
		{
			foreach (var packageName in packageNames)
			{
				Console.WriteLine("|> Processing " + packageName);

				var backupLocation = NewDataPath(packageName + "-Backup", forceCreateDir: true);
				using (var client = new ThinClient(string.Format("http://localhost:{0}", Port), DatabaseName))
				{
					var generator = new DataGenerator(client, 10);

					using (DeployServer(packageName))
					{
						client.PutDatabase(DatabaseName);

						generator.Generate();

						client.StartBackup(DatabaseName, backupLocation, waitForBackupToComplete: true);
					}

					using (var store = NewRemoteDocumentStore(runInMemory: false))
					{
						store.DefaultDatabase = "Northwind";

						var operation = store
							.DatabaseCommands
							.GlobalAdmin
							.StartRestore(new DatabaseRestoreRequest
							{
								BackupLocation = backupLocation,
								DatabaseName = "Northwind"
							});

						operation.WaitForCompletion();

						ValidateBackup(store, generator);
					}
				}
			}
		}

		private static void ValidateBackup(IDocumentStore store, DataGenerator generator)
		{
			WaitForIndexingOfLargeDatabase(store, timeout: TimeSpan.FromMinutes(10));

			ValidateCounts(store, generator);
			ValidateIndexes(store, generator);
		}

		private static void WaitForIndexingOfLargeDatabase(IDocumentStore store, TimeSpan timeout)
		{
			var databaseOpenTimeout = TimeSpan.FromMinutes(1);
			var stopWatch = Stopwatch.StartNew();
			while (true)
			{
				try
				{
					WaitForIndexing(store, timeout: timeout);
					return;
				}
				catch (Exception)
				{
					if (stopWatch.Elapsed >= databaseOpenTimeout) 
						throw;
				}
			}
		}

		private static void ValidateIndexes(IDocumentStore store, DataGenerator generator)
		{
			ValidateOrdersByCompany(store);
			ValidateProductSales(store);
			ValidateOrdersTotals(store, generator);
			ValidateOrdersByEmployeeAndCompany(store, generator);
			ValidateOrdersByEmployeeAndCompanyReduce(store, generator);
		}

		private static void ValidateOrdersByEmployeeAndCompany(IDocumentStore store, DataGenerator generator)
		{
			using (var session = store.OpenSession())
			{
				var count = session
					.Query<Order, OrdersByEmployeeAndCompany>()
					.Count();

				Assert.Equal(generator.ExpectedNumberOfOrders, count);
			}
		}

		private static void ValidateOrdersByEmployeeAndCompanyReduce(IDocumentStore store, DataGenerator generator)
		{
			using (var session = store.OpenSession())
			{
				var count = session
					.Query<OrdersByEmployeeAndCompanyReduce.Result, OrdersByEmployeeAndCompanyReduce>()
					.Count();

				Assert.Equal(generator.ExpectedNumberOfOrders, count);
			}
		}

		private static void ValidateOrdersTotals(IDocumentStore store, DataGenerator generator)
		{
			using (var session = store.OpenSession())
			{
				var count = session
					.Query<OrdersTotals.Result, OrdersTotals>()
					.Count();

				Assert.Equal(generator.ExpectedNumberOfOrders, count);
			}
		}

		private static void ValidateProductSales(IDocumentStore store)
		{
			using (var session = store.OpenSession())
			{
				var count = session
					.Query<ProductSales.Result, ProductSales>()
					.Count();

				Assert.True(count > 0);
			}
		}

		private static void ValidateOrdersByCompany(IDocumentStore store)
		{
			using (var session = store.OpenSession())
			{
				var count = session
					.Query<OrdersByCompany.Result, OrdersByCompany>()
					.Count();

				Assert.True(count > 0);
			}
		}

		private static void ValidateCounts(IDocumentStore store, DataGenerator generator)
		{
			using (var session = store.OpenSession())
			{
				var statistics = store.DatabaseCommands.GetStatistics();
				Assert.Equal(0, statistics.CountOfAttachments);
				Assert.Equal(generator.ExpectedNumberOfIndexes, statistics.Indexes.Length);

				Assert.Equal(generator.ExpectedNumberOfOrders, session.Query<Order>().Count());
				Assert.Equal(generator.ExpectedNumberOfCompanies, session.Query<Company>().Count());
				Assert.Equal(generator.ExpectedNumberOfProducts, session.Query<Product>().Count());
				Assert.Equal(generator.ExpectedNumberOfEmployees, session.Query<Employee>().Count());

				var total = generator.ExpectedNumberOfCompanies + generator.ExpectedNumberOfEmployees + generator.ExpectedNumberOfOrders + generator.ExpectedNumberOfProducts;

				Assert.Equal(total, session.Query<dynamic, RavenDocumentsByEntityName>().Count());
			}
		}

		private ServerRunner DeployServer(string packageName)
		{
			var serverDirectory = NewDataPath(packageName, true);
			IOExtensions.CopyDirectory("../../../packages/" + packageName + "/tools/", serverDirectory);

			return ServerRunner.Run(Port, Path.Combine(serverDirectory, "Raven.Server.exe"));
		}
	}
}