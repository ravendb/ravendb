// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1497 .cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using System.Text;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Database;
using Raven.Database.Actions;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Tests.Common;

using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Issues
{
	public class RavenDB_1497 : RavenTest
	{
		private readonly string BackupDir;
		private readonly string DataDir;

		public class User
		{
			public string Name { get; set; }
			public string Country { get; set; }
		}

		public RavenDB_1497()
		{
			DataDir = NewDataPath("DataDirectory");
			BackupDir = NewDataPath("BackupDatabase");

			IOExtensions.DeleteDirectory(BackupDir);
			IOExtensions.DeleteDirectory(DataDir);
		}

		protected override void ModifyConfiguration(InMemoryRavenConfiguration configuration)
		{
            configuration.Settings[Constants.Esent.CircularLog] = "false";
            configuration.Settings[Constants.Voron.AllowIncrementalBackups] = "true"; //for now all tests run under Voron - so this is needed
			configuration.RunInUnreliableYetFastModeThatIsNotSuitableForProduction = false;
		}

		public class Users_ByName : AbstractIndexCreationTask<User> 
		{
			public Users_ByName()
			{
				Map = users => from u in users select new {u.Name};
			}
		}

		public class Users_ByNameAndCountry : AbstractIndexCreationTask<User>
		{
			public Users_ByNameAndCountry()
			{
				Map = users => from u in users select new { u.Name, u.Country };
			}
		}

		[Fact]
		public void AfterRestoreOfIncrementalBackupAllIndexesShouldWork()
		{
			using(var store = NewDocumentStore(requestedStorage: "esent"))
			{
				new Users_ByName().Execute(store);

				using (var session = store.OpenSession())
				{
					session.Store(new User {Name = "Arek", Country = "Poland"});
					session.SaveChanges();
                }

                WaitForIndexing(store);

				store.SystemDatabase.Maintenance.StartBackup(BackupDir, true, new DatabaseDocument());
				WaitForBackup(store.SystemDatabase, true);

				Thread.Sleep(1000); // incremental tag has seconds precision

				using (var session = store.OpenSession())
				{
					session.Store(new User {Name = "Ramon", Country = "Spain"});
					session.SaveChanges();
				}

				WaitForIndexing(store);

				store.SystemDatabase.Maintenance.StartBackup(BackupDir, true, new DatabaseDocument());
				WaitForBackup(store.SystemDatabase, true);

				Thread.Sleep(1000); // incremental tag has seconds precision

				new Users_ByNameAndCountry().Execute(store);

				WaitForIndexing(store);

				store.SystemDatabase.Maintenance.StartBackup(BackupDir, true, new DatabaseDocument());
				WaitForBackup(store.SystemDatabase, true);

				var output = new StringBuilder();

				MaintenanceActions.Restore(new RavenConfiguration
				{
					Settings =
				{
					{Constants.Esent.CircularLog, "false"},
					{Constants.Voron.AllowIncrementalBackups, "true"}
				}

				}, new DatabaseRestoreRequest
				{
				    BackupLocation = BackupDir,
                    Defrag = true,
                    DatabaseLocation = DataDir
				}, s => output.Append(s));

				Assert.DoesNotContain("error", output.ToString().ToLower());

				using (var db = new DocumentDatabase(new RavenConfiguration
				{
					DataDirectory = DataDir,
					Settings =
					{
						{Constants.Esent.CircularLog, "false"}
					}
				}, null))
				{
					var indexStats = db.Statistics.Indexes;

					Assert.Equal(3, indexStats.Length); // Users/* and Raven/DocumentsByEntityName 

					QueryResult docs = db.Queries.Query("Users/ByName", new IndexQuery
					{
						Query = "Name:*",
						Start = 0,
						PageSize = 10
					}, CancellationToken.None);

					Assert.Equal(2, docs.Results.Count);

					docs = db.Queries.Query("Users/ByNameAndCountry", new IndexQuery
					{
						Query = "Name:*",
						Start = 0,
						PageSize = 10
					}, CancellationToken.None);

					Assert.Equal(2, docs.Results.Count);

				}
			}
		}
	}
}