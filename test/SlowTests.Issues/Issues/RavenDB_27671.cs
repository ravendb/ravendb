using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Raven.Server.Utils;
using Sparrow.Server.Utils;
using Tests.Infrastructure;
using Xunit;
using ServerBackupConfiguration = Raven.Server.Config.Categories.BackupConfiguration;

namespace SlowTests.Issues;

public class RavenDB_27671 : RavenTestBase
{
    // several MB of documents, so zstd splits the stream into multiple jobs when compressing with workers
    private const int NumberOfCompanies = 30_000;

    public RavenDB_27671(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Configuration)]
    [InlineData(1, 0)]
    [InlineData(7, 0)]
    [InlineData(8, 2)]
    [InlineData(16, 2)]
    [InlineData(17, 4)]
    [InlineData(128, 4)]
    public void DefaultZstdCompressionWorkersDependOnTheNumberOfCores(int processorCount, int expectedWorkers)
    {
        Assert.Equal(expectedWorkers, ServerBackupConfiguration.GetDefaultZstdCompressionWorkers(processorCount));
    }

    [RavenTheory(RavenTestCategory.BackupExportImport | RavenTestCategory.Compression)]
    [InlineData(BackupType.Backup, 0)]
    [InlineData(BackupType.Backup, 4)]
    [InlineData(BackupType.Snapshot, 0)]
    [InlineData(BackupType.Snapshot, 4)]
    public async Task CanBackupAndRestoreWithZstdCompressionWorkers(BackupType backupType, int workers)
    {
        var backupPath = NewDataPath(suffix: "BackupFolder");
        IOExtensions.DeleteDirectory(backupPath);

        using (var store = GetDocumentStore(new Options
        {
            ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(x => x.Backup.ZstdCompressionWorkers)] = workers.ToString()
        }))
        {
            var database = await GetDatabase(store.Database);
            Assert.Equal(workers, database.Configuration.Backup.ZstdCompressionWorkers);

            await InsertCompaniesAsync(store);

            // a one-time backup goes through the same backup task as periodic ones, but leaves no backup task in the database record
            // (a periodic task would be restored with the record and expose the test to RavenDB-27674)
            var backupOperation = await store.Maintenance.SendAsync(new BackupOperation(new BackupConfiguration
            {
                BackupType = backupType,
                LocalSettings = new LocalSettings { FolderPath = backupPath }
            }));
            var backupResult = (BackupResult)await backupOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(2));
            if (backupType == BackupType.Backup)
                Assert.Equal(NumberOfCompanies, backupResult.Documents.ReadCount);

            var backupDirectory = Directory.GetDirectories(backupPath).First();
            var lastFile = Directory.GetFiles(backupDirectory)
                .Where(Raven.Client.Documents.Smuggler.BackupUtils.IsFullBackupOrSnapshot)
                .OrderBackups()
                .Last();

            var restoredDatabaseName = GetDatabaseName() + "_restored";
            var restoreOperation = new RestoreBackupOperation(new RestoreBackupConfiguration
            {
                BackupLocation = backupDirectory,
                DatabaseName = restoredDatabaseName,
                LastFileNameToRestore = lastFile
            });

            var operation = await store.Maintenance.Server.SendAsync(restoreOperation);
            var restoreResult = await operation.WaitForCompletionAsync<RestoreResult>(TimeSpan.FromMinutes(2));
            if (backupType == BackupType.Backup)
                Assert.Equal(NumberOfCompanies, restoreResult.Documents.ReadCount);

            using (var restored = GetDocumentStore(new Options
            {
                CreateDatabase = false,
                ModifyDatabaseName = _ => restoredDatabaseName
            }))
            {
                await AssertCompaniesAsync(restored);
            }
        }
    }

    [RavenTheory(RavenTestCategory.BackupExportImport | RavenTestCategory.Compression)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All, Data = new object[] { 0 })]
    [RavenData(DatabaseMode = RavenDatabaseMode.All, Data = new object[] { 4 })]
    public async Task CanExportAndImportWithZstdCompressionWorkers(Options options, int workers)
    {
        var exportPath = NewDataPath(suffix: "ExportFolder");
        IOExtensions.DeleteDirectory(exportPath);
        var exportFile = Path.Combine(exportPath, "export.ravendbdump");

        options.ModifyDatabaseRecord += record => record.Settings[RavenConfiguration.GetKey(x => x.ExportImport.ZstdCompressionWorkers)] = workers.ToString();

        using (var store = GetDocumentStore(options))
        {
            await InsertCompaniesAsync(store);

            var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), exportFile);
            await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(2));
        }

        await using (var fileStream = File.OpenRead(exportFile))
        await using (var exportStream = await Raven.Server.Utils.BackupUtils.GetDecompressionStreamAsync(fileStream))
            Assert.IsType<Sparrow.Utils.ZstdStream>(exportStream);

        using (var store = GetDocumentStore(options))
        {
            var operation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), exportFile);
            await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(2));

            await AssertCompaniesAsync(store);
        }
    }

    private static async Task InsertCompaniesAsync(IDocumentStore store)
    {
        await using (var bulkInsert = store.BulkInsert())
        {
            for (var i = 0; i < NumberOfCompanies; i++)
            {
                await bulkInsert.StoreAsync(new Company
                {
                    Name = $"Company {i}",
                    Phone = $"({i % 1000:000}) 555-{i % 10000:0000}",
                    Address = new Address { City = $"City {i % 97}", Country = $"Country {i % 13}", PostalCode = $"{i:00000}" }
                }, $"companies/{i}");
            }
        }
    }

    private static async Task AssertCompaniesAsync(IDocumentStore store)
    {
        // collection statistics work for sharded databases too
        var stats = await store.Maintenance.SendAsync(new GetCollectionStatisticsOperation());
        Assert.Equal(NumberOfCompanies, stats.CountOfDocuments);

        using (var session = store.OpenAsyncSession())
        {
            foreach (var i in new[] { 0, NumberOfCompanies / 2, NumberOfCompanies - 1 })
            {
                var company = await session.LoadAsync<Company>($"companies/{i}");
                Assert.Equal($"Company {i}", company.Name);
                Assert.Equal($"{i:00000}", company.Address.PostalCode);
            }
        }
    }
}
