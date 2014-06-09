// -----------------------------------------------------------------------
//  <copyright file="CoreTestServer.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading.Tasks;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace Raven.Tests.Core.Commands
{
	public class Other : RavenCoreTestBase
	{
		[Fact]
		public async Task CanGetBuildNumber()
		{
			using (var store = GetDocumentStore())
			{
				var buildNumber = await store.AsyncDatabaseCommands.GetBuildNumberAsync();

				Assert.NotNull(buildNumber);
			}
		}

		[Fact]
		public async Task CanGetStatistics()
		{
			using (var store = GetDocumentStore())
			{
				var databaseStatistics = await store.AsyncDatabaseCommands.GetStatisticsAsync();

				Assert.NotNull(databaseStatistics);

				Assert.Equal(0, databaseStatistics.CountOfDocuments);
			}
		}

		[Fact]
		public async Task CanGetAListOfDatabasesAsync()
		{
			using (var store = GetDocumentStore())
			{
				var names = await store.AsyncDatabaseCommands.ForSystemDatabase().GetDatabaseNamesAsync(25);
				Assert.Contains(store.DefaultDatabase, names);
			}
		}

        [Fact]
        public void CanSwitchDatabases()
        {
        }
	}
}
