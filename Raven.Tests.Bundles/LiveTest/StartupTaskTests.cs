// -----------------------------------------------------------------------
//  <copyright file="StartupTaskTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.ComponentModel.Composition.Hosting;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Data;
using Raven.Bundles.LiveTest;
using Raven.Client.Embedded;
using Raven.Database.Config;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bundles.LiveTest
{
	public class StartupTaskTests : RavenTest
	{
		protected override void ModifyConfiguration(InMemoryRavenConfiguration configuration)
		{
			configuration.Catalog.Catalogs.Add(new AssemblyCatalog(typeof(LiveTestDatabaseDocumentPutTrigger).Assembly));
		}

		[Fact]
		public void CleanerStartupTaskShouldRemoveDatabasesAfterIdleTimeout()
		{
			using (var store = NewDocumentStore())
			{
				store
					.DatabaseCommands
					.GlobalAdmin
					.CreateDatabase(new DatabaseDocument
					{
						Id = "Northwind",
						Settings =
						{
							{ "Raven/ActiveBundles", "Replication" },
							{ "Raven/DataDir", NewDataPath() }
						}
					});

				store
					.DatabaseCommands
					.GlobalAdmin
					.CreateDatabase(new DatabaseDocument
					{
						Id = "Northwind2",
						Settings =
						{
							{ "Raven/ActiveBundles", "Replication" },
							{ "Raven/DataDir", NewDataPath() }
						}
					});

				store
					.DatabaseCommands
					.GlobalAdmin
					.CreateDatabase(new DatabaseDocument
					{
						Id = "Northwind3",
						Settings =
						{
							{ "Raven/ActiveBundles", "Replication" },
							{ "Raven/DataDir", NewDataPath() }
						}
					});

				store
					.DatabaseCommands
					.ForDatabase("Northwind2")
					.GetStatistics();

				store
					.DatabaseCommands
					.ForDatabase("Northwind3")
					.GetStatistics();

				Assert.NotNull(store.DatabaseCommands.Get(Constants.RavenDatabasesPrefix + "Northwind"));
				Assert.NotNull(store.DatabaseCommands.Get(Constants.RavenDatabasesPrefix + "Northwind2"));

				store.ServerIfEmbedded
					.ServerStartupTasks.OfType<LiveTestDatabaseCleanerStartupTask>().First()
					.ExecuteCleanup(null);
				
				Assert.Null(store.DatabaseCommands.Get(Constants.RavenDatabasesPrefix + "Northwind"));
				Assert.NotNull(store.DatabaseCommands.Get(Constants.RavenDatabasesPrefix + "Northwind2"));

				store.ServerIfEmbedded.Server.Options.DatabaseLandlord.LastRecentlyUsed["Northwind2"] = DateTime.MinValue;

				store.ServerIfEmbedded
					.ServerStartupTasks.OfType<LiveTestDatabaseCleanerStartupTask>().First()
					.ExecuteCleanup(null);
				
				Assert.Null(store.DatabaseCommands.Get(Constants.RavenDatabasesPrefix + "Northwind2"));
				Assert.NotNull(store.DatabaseCommands.Get(Constants.RavenDatabasesPrefix + "Northwind3"));
			}
		}
	}
}