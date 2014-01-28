using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Web.UI.WebControls.WebParts;
using Lucene.Net.Store;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Smuggler;
using Raven.Client.Extensions;
using Raven.Database.Bundles.PeriodicBackups;
using Raven.Database.Config;
using Raven.Json.Linq;
using Raven.Smuggler;
using Raven.Tests.Helpers;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Issues
{
	public class RavenDB_1594 : RavenTestBase
	{
		public class DummyDataEntry
		{
			public string Id { get; set; }

			public string Data { get; set; }
		}

		protected override void ModifyConfiguration(InMemoryRavenConfiguration configuration)
		{
			configuration.Settings["Raven/ActiveBundles"] = "PeriodicBackup";
		}

		[Theory]
		[InlineData("esent")]
		//[InlineData("munin")]
		public async Task PeriodicBackup_should_export_all_relevant_documents(string storageTypeName)
		{
			var existingData = new List<DummyDataEntry>();
			var backupFolder = new DirectoryInfo(Path.GetTempPath() + "\\periodic_backup_" + Guid.NewGuid());
			if (!backupFolder.Exists)
				backupFolder.Create();

			using (var store = NewRemoteDocumentStore(runInMemory: false, requestedStorage: storageTypeName))
			{
				store.DatabaseCommands.CreateDatabase(new DatabaseDocument
				{
					Id = "SourceDB",
					Settings =
					{
						{"Raven/ActiveBundles", "PeriodicBackup"},
						{"Raven/DataDir", "~\\Databases\\SourceDB"}
					}
				});

				store.DatabaseCommands.CreateDatabase(new DatabaseDocument
				{
					Id = "DestDB",
					Settings = {{"Raven/DataDir", "~\\Databases\\DestDB"}}
				});

			}

			using(var ravenServer = GetNewServer(runInMemory:false,requestedStorage:storageTypeName))
			using (var srcStore = NewRemoteDocumentStore(runInMemory: false, requestedStorage: storageTypeName,ravenDbServer:ravenServer, databaseName: "SourceDB"))
			{
				//setup periodic backup
				using (var session = srcStore.OpenSession("SourceDB"))
				{
					session.Store(new PeriodicBackupSetup { LocalFolderName = backupFolder.FullName, IntervalMilliseconds = 500 },
						PeriodicBackupSetup.RavenDocumentKey);

					session.SaveChanges();
				}

				//now enter dummy data
				using (var session = srcStore.OpenSession())
				{
					for (int i = 0; i < 10000; i++)
					{
						var dummyDataEntry = new DummyDataEntry {Id = "Dummy/" + i, Data = "Data-" + i};
						existingData.Add(dummyDataEntry);
						session.Store(dummyDataEntry);
					}
					session.SaveChanges();
				}
			
				Thread.Sleep(10000);
			}

			using (var destStore = NewRemoteDocumentStore(runInMemory: false, requestedStorage: storageTypeName, databaseName: "DestDB"))
			{
                var smugglerApi = new SmugglerApi(new RavenConnectionStringOptions
                                              {
                                                  Url = destStore.Url,
                                                  DefaultDatabase = "DestDB"
                                              });

			    await smugglerApi.ImportData(new SmugglerImportOptions
			                                 {
			                                     FromFile = backupFolder.FullName
			                                 }, 
                                             new SmugglerOptions
                                             {
                                                 Incremental = true
                                             });

				using (var session = destStore.OpenSession())
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
}
