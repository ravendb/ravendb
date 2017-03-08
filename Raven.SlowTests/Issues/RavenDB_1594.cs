using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Smuggler;
using Raven.Client.Document;
using Raven.Database.Config;
using Raven.Database.Extensions;
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

        public RavenDB_1594()
        {
            path = NewDataPath();
            pathsToDelete.Add("~/Databases");
            Raven.Database.Extensions.IOExtensions.DeleteDirectory(path);
            var config = new Raven.Database.Config.RavenConfiguration
                            {
                                Port = 8079,
                                RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true,
                                MaxSecondsForTaskToWaitForDatabaseToLoad = 20,
                                DataDirectory = path,
                                Settings = { { "Raven/ActiveBundles", "PeriodicBackup" } },
                            };
            config.PostInit();
            ravenDbServer = new RavenDbServer(config)
            {
                UseEmbeddedHttpServer = true,
            };
            ravenDbServer.Configuration.MaxSecondsForTaskToWaitForDatabaseToLoad = 30;

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

        [Fact, Trait("Category", "Smuggler")]
        public async Task PeriodicBackup_should_export_all_relevant_documents()
        {
            var existingData = new List<DummyDataEntry>();
            var backupFolder = new DirectoryInfo(Path.GetTempPath() + "\\periodic_backup_" + Guid.NewGuid());
            if (backupFolder.Exists == false)
                backupFolder.Create();

            try
            {
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
                    Id = "TestDB",
                    Settings = {{"Raven/DataDir", "~\\Databases\\TestDB"}}
                });
                
                //now enter dummy data
                using (var session = documentStore.OpenSession("SourceDB"))
                {
                    for (int i = 0; i < 10000; i++)
                    {
                        var dummyDataEntry = new DummyDataEntry {Id = "Dummy/" + i, Data = "Data-" + i};
                        existingData.Add(dummyDataEntry);
                        session.Store(dummyDataEntry);
                    }
                    session.SaveChanges();
                }

                var etag = documentStore.DatabaseCommands.ForDatabase("SourceDB").Get("Dummy/9999").Etag;
                //setup periodic export
                using (var session = documentStore.OpenSession("SourceDB"))
                {
                    session.Store(new PeriodicExportSetup { LocalFolderName = backupFolder.FullName, IntervalMilliseconds = 500 },
                        PeriodicExportSetup.RavenDocumentKey);
                    session.SaveChanges();
                }

                var timeout = Debugger.IsAttached ? TimeSpan.FromMinutes(5) : TimeSpan.FromSeconds(5);
                SpinWait.SpinUntil(() =>
                {
                    var doc = documentStore.DatabaseCommands.ForDatabase("SourceDB").Get(PeriodicExportStatus.RavenDocumentKey);
                    return doc != null && doc.Etag == etag;
                }, timeout);
                
                var connection = new RavenConnectionStringOptions {Url = documentStore.Url, DefaultDatabase = "TestDB" };
                var smugglerApi = new SmugglerDatabaseApi { Options = { Incremental = false } };

                var actualBackupPath = Directory.GetDirectories(backupFolder.FullName)[0];
                var fullBackupFilePath = Directory.GetFiles(actualBackupPath).FirstOrDefault(x => x.Contains("full"));
                Assert.NotNull(fullBackupFilePath);
                
                await smugglerApi.ImportData(new SmugglerImportOptions<RavenConnectionStringOptions> { FromFile = fullBackupFilePath, To = connection });

                using (var session = documentStore.OpenSession("TestDB"))
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
            finally
            {
                IOExtensions.DeleteDirectory(backupFolder.FullName);
            }
        }
    }
}
