// -----------------------------------------------------------------------
//  <copyright file="RavenDB_999.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client.Extensions;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_999 : RavenTest
	{
		[Fact]
		public void CanCreateDatabase()
		{
			using (var store = NewRemoteDocumentStore(databaseName: "Test"))
			{
				const string dbName = "RavenDB_999";
				store.DatabaseCommands.GlobalAdmin.CreateDatabase(new DatabaseDocument()
				{
					Id = dbName,
					Settings =
					{
						{"Raven/DataDir", Path.Combine("~", Path.Combine("Databases", dbName))}
					}
				});

				var databaseNames = store.DatabaseCommands.ForSystemDatabase().GetDatabaseNames(10);

				Assert.Contains(dbName, databaseNames);
			}
		}

		[Fact]
		public async Task CanCreateDatabaseAsync()
		{
			using (var store = NewRemoteDocumentStore(databaseName: "Test"))
			{
				const string dbName = "RavenDB_999";
				await store.AsyncDatabaseCommands.GlobalAdmin.CreateDatabaseAsync(new DatabaseDocument()
				{
					Id = dbName,
					Settings =
					{
						{"Raven/DataDir", Path.Combine("~", Path.Combine("Databases", dbName))}
					}
				});

				var databaseNames = store.DatabaseCommands.ForSystemDatabase().GetDatabaseNames(10);

				Assert.Contains(dbName, databaseNames);
			}
		}

		[Fact]
		public void CanDeleteDatabase()
		{
			using (var store = NewRemoteDocumentStore())
			{
				const string dbName = "RavenDB_999";
				store.DatabaseCommands.GlobalAdmin.EnsureDatabaseExists(dbName);
				store.DatabaseCommands.GlobalAdmin.DeleteDatabase(dbName);

				var databaseNames = store.DatabaseCommands.ForSystemDatabase().GetDatabaseNames(10);

				Assert.DoesNotContain(dbName, databaseNames);
			}
		}

		[Fact]
		public async Task CanDeleteDatabaseAsync()
		{
			using (var store = NewRemoteDocumentStore())
			{
				const string dbName = "RavenDB_999";
				await store.AsyncDatabaseCommands.GlobalAdmin.EnsureDatabaseExistsAsync(dbName);
				await store.AsyncDatabaseCommands.GlobalAdmin.DeleteDatabaseAsync(dbName);

				var databaseNames = store.DatabaseCommands.ForSystemDatabase().GetDatabaseNames(10);

				Assert.DoesNotContain(dbName, databaseNames);
			}
		}

		[Fact]
		public void CanToggleIndexing()
		{
			using (var store = NewRemoteDocumentStore())
			{
				store.DatabaseCommands.Admin.StopIndexing();

				Assert.Equal("Paused", store.DatabaseCommands.Admin.GetIndexingStatus());

				store.DatabaseCommands.Admin.StartIndexing();
				Assert.Equal("Indexing", store.DatabaseCommands.Admin.GetIndexingStatus());
			}
		}

		[Fact]
		public async Task CanToggleIndexingAsync()
		{
			using (var store = NewRemoteDocumentStore())
			{
				await store.AsyncDatabaseCommands.Admin.StopIndexingAsync();

				Assert.Equal("Paused", await store.AsyncDatabaseCommands.Admin.GetIndexingStatusAsync());

				await store.AsyncDatabaseCommands.Admin.StartIndexingAsync();
				Assert.Equal("Indexing", await store.AsyncDatabaseCommands.Admin.GetIndexingStatusAsync());
			}
		}

		[Fact]
		public void CanTakeGlobalAdminStats()
		{
			using (var store = NewRemoteDocumentStore())
			{
				var adminStatistics = store.DatabaseCommands.GlobalAdmin.GetStatistics();

				Assert.NotNull(adminStatistics);
			}
		}

		[Fact]
		public async Task CanTakeGlobalAdminStatsAsync()
		{
			using (var store = NewRemoteDocumentStore())
			{
				var adminStatistics = await store.AsyncDatabaseCommands.GlobalAdmin.GetStatisticsAsync();

				Assert.NotNull(adminStatistics);
			}
		}

		[Fact]
		public void CanCompactDatabase()
		{
			using (var store = NewRemoteDocumentStore(runInMemory: false))
			{
				const string dbName = "RavenDB_999_Compact";

				store.DatabaseCommands.GlobalAdmin.EnsureDatabaseExists(dbName);

				store.DatabaseCommands.Put("keys/1", null, new RavenJObject() { { "test", "test" } }, new RavenJObject());

				store.DatabaseCommands.GlobalAdmin.CompactDatabase(dbName);

				Assert.NotNull(store.DatabaseCommands.Get("keys/1"));
			}
		}

		[Fact]
		public async Task CanCompactDatabaseAsync()
		{
			using (var store = NewRemoteDocumentStore(runInMemory: false))
			{
				const string dbName = "RavenDB_999_CompactAsync";

				await store.AsyncDatabaseCommands.GlobalAdmin.EnsureDatabaseExistsAsync(dbName);

				await store.AsyncDatabaseCommands.PutAsync("keys/1", null, new RavenJObject() {{"test", "test"}}, new RavenJObject());

				await store.AsyncDatabaseCommands.GlobalAdmin.CompactDatabaseAsync(dbName);

				Assert.NotNull(await store.AsyncDatabaseCommands.GetAsync("keys/1"));
			}
		}
	}
}