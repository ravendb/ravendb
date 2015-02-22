// -----------------------------------------------------------------------
//  <copyright file="MigrationTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Xml.Linq;

using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Tests.Common;
using Raven.Tests.Migration.Indexes;
using Raven.Tests.Migration.Utils;
using Raven.Tests.Migration.Utils.Orders;

using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Migration
{
	public class MigrationTests : RavenTest
	{
		protected override void ModifyConfiguration(InMemoryRavenConfiguration configuration)
		{
			configuration.MaxPageSize = int.MaxValue;
		}

		private const string ServersFileName = "servers.txt";

		private const int Port = 8075;

		private const string DatabaseName = "Northwind";

		private const string PackageName = "RavenDB.Server";

		private readonly List<ServerConfiguration> serversToTest;

		public MigrationTests()
		{
			serversToTest = new List<ServerConfiguration>();

			foreach (var version in ParseServers())
			{
				var configuration = new ServerConfiguration { Name = PackageName + "." + version };

				if (version.StartsWith("2.0") || version.StartsWith("2.5"))
				{
					configuration.WaitForIndexingBeforeBackup = true;
					configuration.StorageTypes = new[] { "esent" };
				}

				if (version.StartsWith("3.0"))
				{
					configuration.WaitForIndexingBeforeBackup = true;
					configuration.StorageTypes = new[] { "esent", "voron" };
				}

				serversToTest.Add(configuration);
			}
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

				yield return attributes.First(x => x.Name == "version").Value;
			}
		}

		[Theory(Skip = "Long running migration test")]
		[InlineData(10, "voron")]
		[InlineData(10, "esent")]
		[InlineData(20, "voron")]
		[InlineData(20, "esent")]
		[InlineData(100, "voron")]
		[InlineData(100, "esent")]
		public void LargeMigrationSelf(int numberOfIterations, string requestedStorage)
		{
			var backupLocation = NewDataPath("Self-Backup", forceCreateDir: true);

			DataGenerator generator;

			using (var store = NewRemoteDocumentStore(runInMemory: false, requestedStorage: requestedStorage))
			{
				using (var client = new ThinClient(store.Url, DatabaseName))
				{
					generator = new DataGenerator(client, numberOfIterations);

					client.PutDatabase(DatabaseName);

					generator.Generate();

					client.StartBackup(DatabaseName, backupLocation, waitForBackupToComplete: true);
				}
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

		[Theory(Skip = "Long running migration test")]
		[InlineData(10)]
		[InlineData(20)]
		[InlineData(100)]
		public void LargeMigration(int numberOfIterations)
		{
			var exceptions = new List<Exception>();

			foreach (var configuration in serversToTest)
				foreach (var storageType in configuration.StorageTypes)
				{
					KillPreviousServers();

					Console.WriteLine("|> Processing: {0}. Storage: {1}.", configuration.Name, storageType);

					var backupLocation = NewDataPath(configuration.Name + "-Backup", forceCreateDir: true);
					using (var client = new ThinClient(string.Format("http://localhost:{0}", Port), DatabaseName))
					{
						var generator = new DataGenerator(client, numberOfIterations);

						using (var server = DeployServer(configuration.Name, storageType))
						{
							File.AppendAllLines(ServersFileName, new[] { server.ProcessId.ToString(CultureInfo.InvariantCulture) });

							client.PutDatabase(DatabaseName);

							generator.Generate();

							if (configuration.WaitForIndexingBeforeBackup)
								client.WaitForIndexing();

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

							try
							{
								ValidateBackup(store, generator);
							}
							catch (Exception e)
							{
								Console.WriteLine(e);

								exceptions.Add(new InvalidOperationException("Migration failed: " + configuration.Name + ". Storage: " + storageType + ". Message: " + e.Message, e));
							}
						}
					}
				}

			if (exceptions.Count > 0)
				throw new AggregateException(exceptions);
		}

		private static void KillPreviousServers()
		{
			if (File.Exists(ServersFileName) == false)
				return;

			try
			{
				foreach (var processId in File.ReadAllLines(ServersFileName))
				{
					try
					{
						var process = Process.GetProcessById(int.Parse(processId));
						if (string.Equals(process.ProcessName, "Raven.Server", StringComparison.OrdinalIgnoreCase))
							process.Kill();
					}
					catch
					{
					}
				}
			}
			finally
			{
				File.Delete(ServersFileName);
			}
		}

		private static void ValidateBackup(IDocumentStore store, DataGenerator generator)
		{
			WaitForIndexingOfLargeDatabase(store, timeout: TimeSpan.FromMinutes(60));

			ValidateCounts(store, generator);
			ValidateIndexes(store, generator);
		}

		private static void WaitForIndexingOfLargeDatabase(IDocumentStore store, TimeSpan timeout)
		{
			var databaseOpenTimeout = TimeSpan.FromMinutes(5);
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

				Thread.Sleep(1000);
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

				//Console.WriteLine(RavenJObject.FromObject(statistics).ToString(Formatting.Indented));

				Assert.Equal(0, statistics.Errors.Length);
				Assert.Equal(0, statistics.StaleIndexes.Length);
				Assert.Equal(generator.ExpectedNumberOfIndexes, statistics.Indexes.Length);
				Assert.Equal(0, statistics.CountOfAttachments);

				Assert.Equal(generator.ExpectedNumberOfOrders, session.Query<Order>().Count());
				Assert.Equal(generator.ExpectedNumberOfCompanies, session.Query<Company>().Count());
				Assert.Equal(generator.ExpectedNumberOfProducts, session.Query<Product>().Count());
				Assert.Equal(generator.ExpectedNumberOfEmployees, session.Query<Employee>().Count());

				var total = generator.ExpectedNumberOfCompanies + generator.ExpectedNumberOfEmployees + generator.ExpectedNumberOfOrders + generator.ExpectedNumberOfProducts;

				Assert.Equal(total, session.Query<dynamic, RavenDocumentsByEntityName>().Count());
				Assert.Equal(total, statistics.CountOfDocuments);
			}
		}

		private ServerRunner DeployServer(string packageName, string storageType)
		{
			var serverDirectory = NewDataPath(packageName, true);
			IOExtensions.CopyDirectory("../../../packages/" + packageName + "/tools/", serverDirectory);

			return ServerRunner.Run(Port, storageType, Path.Combine(serverDirectory, "Raven.Server.exe"));
		}

		private class ServerConfiguration
		{
			public string Name { get; set; }

			public bool WaitForIndexingBeforeBackup { get; set; }

			public string[] StorageTypes { get; set; }
		}
	}
}