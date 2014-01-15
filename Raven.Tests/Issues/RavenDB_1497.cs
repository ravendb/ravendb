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
using Raven.Database.Config;
using Raven.Database.Extensions;
using Xunit;

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
			configuration.Settings["Raven/Esent/CircularLog"] = "false";
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

				store.DocumentDatabase.StartBackup(BackupDir, true, new DatabaseDocument());
				WaitForBackup(store.DocumentDatabase, true);

				Thread.Sleep(1000); // incremental tag has seconds precision

				using (var session = store.OpenSession())
				{
					session.Store(new User {Name = "Ramon", Country = "Spain"});
					session.SaveChanges();
				}

				WaitForIndexing(store);

				store.DocumentDatabase.StartBackup(BackupDir, true, new DatabaseDocument());
				WaitForBackup(store.DocumentDatabase, true);

				Thread.Sleep(1000); // incremental tag has seconds precision

				new Users_ByNameAndCountry().Execute(store);

				WaitForIndexing(store);

				store.DocumentDatabase.StartBackup(BackupDir, true, new DatabaseDocument());
				WaitForBackup(store.DocumentDatabase, true);

				var output = new StringBuilder();

				DocumentDatabase.Restore(new RavenConfiguration
				{
					Settings =
				{
					{"Raven/Esent/CircularLog", "false"}
				}

				}, BackupDir, DataDir, s => output.Append(s), defrag: true);

				Assert.DoesNotContain("error", output.ToString().ToLower());

				using (var db = new DocumentDatabase(new RavenConfiguration
				{
					DataDirectory = DataDir,
					Settings =
					{
						{"Raven/Esent/CircularLog", "false"}
					}
				}))
				{
					var indexStats = db.Statistics.Indexes;

					Assert.Equal(3, indexStats.Length); // Users/* and Raven/DocumentsByEntityName 

					QueryResult docs = db.Query("Users/ByName", new IndexQuery
					{
						Query = "Name:*",
						Start = 0,
						PageSize = 10
					});

					Assert.Equal(2, docs.Results.Count);

					docs = db.Query("Users/ByNameAndCountry", new IndexQuery
					{
						Query = "Name:*",
						Start = 0,
						PageSize = 10
					});

					Assert.Equal(2, docs.Results.Count);

				}
				//QueryResult docs = null;

				//for (int i = 0; i < 500; i++)
				//{
				//	docs = db.Query("Users/ByName", new IndexQuery
				//	{
				//		Query = "Name:*",
				//		Start = 0,
				//		PageSize = 10
				//	});
				//	if (docs.IsStale == false)
				//		break;

				//	Thread.Sleep(100);
				//}

				//Assert.NotNull(docs);
				//Assert.Equal(2, docs.Results.Count);

				//var jObject = db.Get("ayende", null).ToJson();
				//Assert.Equal("ayende@ayende.com", jObject.Value<string>("email"));
				//jObject = db.Get("itamar", null).ToJson();
				//Assert.Equal("itamar@ayende.com", jObject.Value<string>("email"));
			}
		}
	}
}