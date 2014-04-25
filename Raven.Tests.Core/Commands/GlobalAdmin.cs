// -----------------------------------------------------------------------
//  <copyright file="GlobalAdmin.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Client.Extensions;
using Xunit;

namespace Raven.Tests.Core.Commands
{
	public class GlobalAdmin : RavenCoreTestBase
	{
		[Fact]
		public void CanCreateAndDeleteDatabase()
		{
			using (var store = GetDocumentStore())
			{
				var globalAdmin = store.DatabaseCommands.GlobalAdmin;

				const string databaseName = "SampleDb_Of_CanCreateAndDeleteDatabase_Test";

				var databaseDocument = MultiDatabase.CreateDatabaseDocument(databaseName);

				globalAdmin.CreateDatabase(databaseDocument);

				Assert.NotNull(store.DatabaseCommands.ForSystemDatabase().Get(databaseDocument.Id));

				globalAdmin.DeleteDatabase(databaseName, true);

				Assert.Null(store.DatabaseCommands.ForSystemDatabase().Get(databaseDocument.Id));
			}
		}

		[Fact]
		public void CanGetServerStatistics()
		{
			using (var store = GetDocumentStore())
			{
				var globalAdmin = store.DatabaseCommands.GlobalAdmin;
				
				var adminStatistics = globalAdmin.GetStatistics();

				Assert.NotNull(adminStatistics);

				Assert.Equal(TestServerFixture.ServerName, adminStatistics.ServerName);
				Assert.True(adminStatistics.LoadedDatabases.Any());
				Assert.True(adminStatistics.TotalNumberOfRequests > 0);
				Assert.NotNull(adminStatistics.Memory);
			}
		}
	}
}