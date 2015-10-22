// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3759.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;
using System.Threading.Tasks;

using Raven.Abstractions.Data;
using Raven.Abstractions.Smuggler;
using Raven.Smuggler;
using Raven.Smuggler.Database;
using Raven.Smuggler.Database.Impl.Files;
using Raven.Smuggler.Database.Impl.Remote;
using Raven.Smuggler.Database.Impl.Streams;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;
using Raven.Tests.Smuggler.Helpers;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_3759 : RavenTest
	{
		[Fact]
		public async Task NorthwindStreamReadBasicTest()
		{
			using (var store = NewRemoteDocumentStore())
			{
				DeployNorthwind(store);

				WaitForIndexing(store);

				using (var input = new MemoryStream())
				{
					var oldSmuggler = new SmugglerDatabaseApi();
					await oldSmuggler
						.ExportData(new SmugglerExportOptions<RavenConnectionStringOptions>
						{
							From = new RavenConnectionStringOptions
							{
								DefaultDatabase = store.DefaultDatabase,
								Url = store.Url
							},
							ToStream = input
						});

					var destination = new DatabaseSmugglerCountingDestination();
					var smuggler = new DatabaseSmuggler(new DatabaseSmugglerOptions(), new DatabaseSmugglerStreamSource(input), destination);
					await smuggler.ExecuteAsync();

					Assert.Equal(1059, destination.WroteDocuments);
					Assert.Equal(0, destination.WroteDocumentDeletions);
					Assert.Equal(1, destination.WroteIdentities);
					Assert.Equal(4, destination.WroteIndexes);
					Assert.Equal(1, destination.WroteTransformers);
				}
			}
		}

		[Fact]
		public async Task NorthwindFileReadBasicTest()
		{
			using (var store = NewRemoteDocumentStore())
			{
				DeployNorthwind(store);

				WaitForIndexing(store);

				var input = Path.Combine(NewDataPath(forceCreateDir: true), "backup.ravendump");

				var oldSmuggler = new SmugglerDatabaseApi();
				await oldSmuggler
					.ExportData(new SmugglerExportOptions<RavenConnectionStringOptions>
					{
						From = new RavenConnectionStringOptions
						{
							DefaultDatabase = store.DefaultDatabase,
							Url = store.Url
						},
						ToFile = input
					});

				var destination = new DatabaseSmugglerCountingDestination();
				var smuggler = new DatabaseSmuggler(new DatabaseSmugglerOptions(), new DatabaseSmugglerFileSource(input), destination);
				await smuggler.ExecuteAsync();

				Assert.Equal(1059, destination.WroteDocuments);
				Assert.Equal(0, destination.WroteDocumentDeletions);
				Assert.Equal(1, destination.WroteIdentities);
				Assert.Equal(4, destination.WroteIndexes);
				Assert.Equal(1, destination.WroteTransformers);
			}
		}

		[Fact]
		public async Task NorthwindRemoteReadBasicTest()
		{
			using (var store = NewRemoteDocumentStore())
			{
				DeployNorthwind(store);

				WaitForIndexing(store);

				var destination = new DatabaseSmugglerCountingDestination();
				var smuggler = new DatabaseSmuggler(new DatabaseSmugglerOptions(), new DatabaseSmugglerRemoteSource(store), destination);
				await smuggler.ExecuteAsync();

				Assert.Equal(1059, destination.WroteDocuments);
				Assert.Equal(0, destination.WroteDocumentDeletions);
				Assert.Equal(1, destination.WroteIdentities);
				Assert.Equal(4, destination.WroteIndexes);
				Assert.Equal(1, destination.WroteTransformers);
			}
		}

		[Fact]
		public async Task NorthwindFileReadIncrementalTest()
		{
			using (var store = NewRemoteDocumentStore())
			{
				DeployNorthwind(store);

				WaitForIndexing(store);

				var input = NewDataPath(forceCreateDir: true);

				var oldSmuggler = new SmugglerDatabaseApi(new SmugglerDatabaseOptions { Incremental = true });
				await oldSmuggler
					.ExportData(new SmugglerExportOptions<RavenConnectionStringOptions>
					{
						From = new RavenConnectionStringOptions
						{
							DefaultDatabase = store.DefaultDatabase,
							Url = store.Url
						},
						ToFile = input
					});

				using (var session = store.OpenSession())
				{
					session.Store(new Person { Name = "John Doe" });
					session.SaveChanges();
				}

				await oldSmuggler
					.ExportData(new SmugglerExportOptions<RavenConnectionStringOptions>
					{
						From = new RavenConnectionStringOptions
						{
							DefaultDatabase = store.DefaultDatabase,
							Url = store.Url
						},
						ToFile = input
					});

				var destination = new DatabaseSmugglerCountingDestination();
				var smuggler = new DatabaseSmuggler(new DatabaseSmugglerOptions(), new DatabaseSmugglerFileSource(input), destination);
				await smuggler.ExecuteAsync();

				Assert.Equal(1061, destination.WroteDocuments);
				Assert.Equal(0, destination.WroteDocumentDeletions);
				Assert.Equal(2, destination.WroteIdentities);
				Assert.Equal(4, destination.WroteIndexes);
				Assert.Equal(1, destination.WroteTransformers);
			}
		}
	}
}