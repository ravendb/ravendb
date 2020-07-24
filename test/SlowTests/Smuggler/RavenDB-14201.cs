using System;
using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations.Migration;
using Raven.Server.Config.Settings;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Smuggler
{
    public class RavenDB_14201 : RavenTestBase
    {
        public RavenDB_14201(ITestOutputHelper output) : base(output)
        {
        }

        [NonLinuxFact]
        public async Task CanOfflineMigrateVoronBackup()
        {
            string dataDir = UnzipTheZips("SampleDataVoron.zip", out PathSetting storageExplorer);

            var db = new DatabaseRecord($"restoredSampleDataVoron_{Guid.NewGuid()}");
            var config = new OfflineMigrationConfiguration(dataDir, storageExplorer.FullPath, db);
            using (var store = GetDocumentStore(new Options() { CreateDatabase = false }))
            {
                var o = await store.Maintenance.Server.SendAsync(new OfflineMigrationOperation(config));
                await o.WaitForCompletionAsync(TimeSpan.FromSeconds(60));

                using (var store2 = GetDocumentStore(new Options() { CreateDatabase = false, ModifyDatabaseName = s => db.DatabaseName }))
                {
                    var stats = await store2.Maintenance.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(1059, stats.CountOfDocuments);
                    Assert.Equal(3, stats.CountOfIndexes);
                }
            }
        }
        
        [NonLinuxFact]
        public async Task DatabaseNameValidation()
        {
            string dataDir = UnzipTheZips("SampleDataEsent.zip", out PathSetting storageExplorer);
            var invalidDbName = "abc(123)@.*.456";
            var db = new DatabaseRecord(invalidDbName);
            
            var config = new OfflineMigrationConfiguration(dataDir, storageExplorer.FullPath, db);
            
            using (var store = GetDocumentStore(new Options() { CreateDatabase = false }))
            {
                var e = await Assert.ThrowsAsync<BadRequestException>(() => store.Maintenance.Server.SendAsync(new OfflineMigrationOperation(config)));
                Assert.Contains($"The name '{invalidDbName}' is not permitted. Only letters, digits and characters ('_', '-', '.') are allowed.",
                    e.Message);
            }
        }

        [NonLinuxFact]
        public async Task CanOfflineMigrateEsentBackup()
        {
            string dataDir = UnzipTheZips("SampleDataEsent.zip", out PathSetting storageExplorer);

            var db = new DatabaseRecord($"restoredSampleDataEsent_{Guid.NewGuid()}");
            var config = new OfflineMigrationConfiguration(dataDir, storageExplorer.FullPath, db);
            using (var store = GetDocumentStore(new Options() { CreateDatabase = false }))
            {
                var o = await store.Maintenance.Server.SendAsync(new OfflineMigrationOperation(config));
                await o.WaitForCompletionAsync(TimeSpan.FromSeconds(60));

                using (var store2 = GetDocumentStore(new Options() { CreateDatabase = false, ModifyDatabaseName = s => db.DatabaseName }))
                {
                    var stats = await store2.Maintenance.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(1059, stats.CountOfDocuments);
                    Assert.Equal(3, stats.CountOfIndexes);
                }
            }
        }

        [NonLinuxFact]
        public async Task CanOfflineMigrateVoronFileSystemBackup()
        {
            string dataDir = UnzipTheZips("FSVoron.zip", out PathSetting storageExplorer);

            var db = new DatabaseRecord($"restoredFSVoronBackup_{Guid.NewGuid()}");
            var config = new OfflineMigrationConfiguration(dataDir, storageExplorer.FullPath, db);
            config.IsRavenFs = true;
            using (var store = GetDocumentStore(new Options() { CreateDatabase = false }))
            {
                var o = await store.Maintenance.Server.SendAsync(new OfflineMigrationOperation(config));
                await o.WaitForCompletionAsync(TimeSpan.FromSeconds(60));

                using (var store2 = GetDocumentStore(new Options() { CreateDatabase = false, ModifyDatabaseName = s => db.DatabaseName }))
                {
                    var stats = await store2.Maintenance.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(4, stats.CountOfDocuments);
                    Assert.Equal(4, stats.CountOfAttachments);
                }
            }
        }

        [NonLinuxFact]
        public async Task CanOfflineMigrateEsentFileSystemBackup()
        {
            string dataDir = UnzipTheZips("FSEsent.zip", out PathSetting storageExplorer);

            var db = new DatabaseRecord($"restoredFSEsentBackup_{Guid.NewGuid()}");
            var config = new OfflineMigrationConfiguration(dataDir, storageExplorer.FullPath, db);
            config.IsRavenFs = true;
            using (var store = GetDocumentStore(new Options() { CreateDatabase = false }))
            {
                var o = await store.Maintenance.Server.SendAsync(new OfflineMigrationOperation(config));
                await o.WaitForCompletionAsync(TimeSpan.FromSeconds(60));

                using (var store2 = GetDocumentStore(new Options() { CreateDatabase = false, ModifyDatabaseName = s => db.DatabaseName }))
                {
                    WaitForUserToContinueTheTest(store2);

                    var stats = await store2.Maintenance.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(4, stats.CountOfDocuments);
                    Assert.Equal(4, stats.CountOfAttachments);
                }
            }
        }

        [NonLinuxFact]
        public async Task CanOfflineMigrateEsentWithCustomizedLogFileSize()
        {
            string dataDir = UnzipTheZips("SampleDataEsentLogFileSize4.zip", out PathSetting storageExplorer);

            var db = new DatabaseRecord($"restoredSampleDataEsentWithCustomizedLogFileSize_{Guid.NewGuid()}");
            var config = new OfflineMigrationConfiguration(dataDir, storageExplorer.FullPath, db)
            {
                LogFileSize = 4
            };
            using (var store = GetDocumentStore(new Options() { CreateDatabase = false }))
            {
                var o = await store.Maintenance.Server.SendAsync(new OfflineMigrationOperation(config));
                await o.WaitForCompletionAsync(TimeSpan.FromSeconds(60));

                using (var store2 = GetDocumentStore(new Options() { CreateDatabase = false, ModifyDatabaseName = s => db.DatabaseName }))
                {
                    var stats = await store2.Maintenance.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(1059, stats.CountOfDocuments);
                    Assert.Equal(3, stats.CountOfIndexes);
                }
            }
        }

        private string UnzipTheZips(string data, out PathSetting storageExplorer)
        {
            var dataDir = NewDataPath(forceCreateDir: true, prefix: Guid.NewGuid().ToString());
            var zipPath = new PathSetting($"Smuggler/Data/{data}");
            Assert.True(File.Exists(zipPath.FullPath));
            ZipFile.ExtractToDirectory(zipPath.FullPath, dataDir);

            var toolsDir = NewDataPath(forceCreateDir: true, prefix: Guid.NewGuid().ToString());
            var zipToolsPath = new PathSetting("Smuggler/Data/Tools.zip");
            Assert.True(File.Exists(zipToolsPath.FullPath));
            ZipFile.ExtractToDirectory(zipToolsPath.FullPath, toolsDir);
            storageExplorer = new PathSetting($"{toolsDir}/Raven.StorageExporter.exe");
            return dataDir;
        }
    }
}
