using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Smuggler;
using Raven.Client.Document;
using Raven.Database.Config;
using Raven.Server;
using Raven.Smuggler;
using Raven.Tests.Common;

using Xunit;

namespace Raven.SlowTests.Issues
{
	public class RavenDB_1594 : RavenTest
	{
		protected readonly string path;
		protected readonly DocumentStore documentStore;
		private readonly RavenDbServer ravenDbServer;
		private bool closed = false;

		public RavenDB_1594()
		{
		    path = NewDataPath();
			Raven.Database.Extensions.IOExtensions.DeleteDirectory(path);
			var config = new Raven.Database.Config.RavenConfiguration
			             	{
			             		Port = 8079,
			             		RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true,
			             		DataDirectory = path,
								Settings = { { "Raven/ActiveBundles", "PeriodicBackup" } },
			             	};
			config.PostInit();
		    ravenDbServer = new RavenDbServer(config)
		    {
		        UseEmbeddedHttpServer = true
		    };
		    ravenDbServer.Initialize();
			documentStore = new DocumentStore
			{
				Url = "http://localhost:8079"
			};
			documentStore.Initialize();
		}

		public override void Dispose()
		{
			documentStore.Dispose();
			ravenDbServer.Dispose();

			base.Dispose();
		}

		public class DummyDataEntry
		{
			public string Id { get; set; }

			public string Data { get; set; }
		}

		protected override void ModifyConfiguration(InMemoryRavenConfiguration configuration)
		{
			configuration.Settings["Raven/ActiveBundles"] = "PeriodicBackup";
		}

		[Fact]
		public async Task PeriodicBackup_should_export_all_relevant_documents()
		{
			var existingData = new List<DummyDataEntry>();
			var backupFolder = new DirectoryInfo(Path.GetTempPath() + "\\periodic_backup_" + Guid.NewGuid());
			if (!backupFolder.Exists)
				backupFolder.Create();

			documentStore.DatabaseCommands.GlobalAdmin.CreateDatabase(new DatabaseDocument
			{
				Id = "SourceDB",
				Settings =
				{
					{"Raven/ActiveBundles", "PeriodicBackup"},
					{"Raven/DataDir", "~\\Databases\\SourceDB"}
				}
			});

			documentStore.DatabaseCommands.GlobalAdmin.CreateDatabase(new DatabaseDocument
			{
				Id = "DestDB",
				Settings = {{"Raven/DataDir", "~\\Databases\\DestDB"}}
			});
			//setup periodic backup
			using (var session = documentStore.OpenSession("SourceDB"))
			{
				session.Store(new PeriodicBackupSetup {LocalFolderName = backupFolder.FullName, IntervalMilliseconds = 500},
					PeriodicBackupSetup.RavenDocumentKey);
				session.SaveChanges();
			}

			//now enter dummy data
			using (var session = documentStore.OpenSession())
			{
				for (int i = 0; i < 10000; i++)
				{
					var dummyDataEntry = new DummyDataEntry {Id = "Dummy/" + i, Data = "Data-" + i};
					existingData.Add(dummyDataEntry);
					session.Store(dummyDataEntry);
				}
				session.SaveChanges();
			}

			var connection = new RavenConnectionStringOptions {Url = documentStore.Url, DefaultDatabase = "DestDB"};
			var smugglerApi = new SmugglerApi();
			await
				smugglerApi.ImportData(new SmugglerImportOptions { FromFile = backupFolder.FullName, To = connection }, new SmugglerOptions { Incremental = true });

			using (var session = documentStore.OpenSession())
			{
				var fetchedData = new List<DummyDataEntry>();
				using (var streamingQuery = session.Advanced.Stream<DummyDataEntry>("Dummy/"))
				{
					while (streamingQuery.MoveNext())
						fetchedData.Add(streamingQuery.Current.Document);
				}

				Assert.Equal(existingData.Count, fetchedData.Count);
				Assert.True(existingData.Select(row => row.Data).ToHashSet().SetEquals(fetchedData.Select(row => row.Data)));
			}

		}
	}
}
