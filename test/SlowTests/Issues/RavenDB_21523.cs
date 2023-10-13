using System;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Raven.Server.Config.Categories;
using Raven.Server.Utils;
using Sparrow.Backups;
using Sparrow.Utils;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using BackupUtils = Raven.Server.Utils.BackupUtils;

namespace SlowTests.Issues;

public class RavenDB_21523 : RavenTestBase
{
    public RavenDB_21523(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.BackupExportImport)]
    [InlineData(ExportCompressionAlgorithm.Gzip)]
    [InlineData(ExportCompressionAlgorithm.Zstd)]
    public async Task CanExportImport(ExportCompressionAlgorithm algorithm)
    {
        var backupPath = NewDataPath(suffix: "BackupFolder");
        IOExtensions.DeleteDirectory(backupPath);
        var exportFile = Path.Combine(backupPath, "export.ravendbdump");

        using (var store = GetDocumentStore(new Options
        {
            ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(x => x.ExportImport.CompressionAlgorithm)] = algorithm.ToString()
        }))
        {
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Company { Name = "HR" }, "companies/1");
                await session.SaveChangesAsync();
            }

            var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), exportFile);
            await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(15));

            Assert.True(File.Exists(exportFile));

            await using (var fileStream = File.OpenRead(exportFile))
            await using (var backupStream = await BackupUtils.GetDecompressionStreamAsync(fileStream))
            {
                var buffer = new byte[1024];
                var read = await backupStream.ReadAsync(buffer, 0, buffer.Length); // validates if we picked appropriate decompression algorithm
                Assert.True(read > 0);

                switch (algorithm)
                {
                    case ExportCompressionAlgorithm.Gzip:
                        Assert.IsType<GZipStream>(backupStream);
                        break;
                    case ExportCompressionAlgorithm.Zstd:
                        Assert.IsType<ZstdStream>(backupStream);
                        break;
                    default:
                        throw new ArgumentOutOfRangeException(nameof(algorithm), algorithm, null);
                }
            }
        }

        using (var store = GetDocumentStore())
        {
            var operation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), exportFile);
            await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(15));

            using (var session = store.OpenAsyncSession())
            {
                var company = await session.LoadAsync<Company>("companies/1");

                Assert.NotNull(company);
                Assert.Equal("HR", company.Name);
            }
        }
    }

    [RavenTheory(RavenTestCategory.BackupExportImport)]
    [InlineData(ExportCompressionAlgorithm.Gzip, ExportCompressionAlgorithm.Zstd)]
    [InlineData(ExportCompressionAlgorithm.Gzip, ExportCompressionAlgorithm.Gzip)]
    [InlineData(ExportCompressionAlgorithm.Gzip, null)]
    public async Task CanExportImport_Client(ExportCompressionAlgorithm defaultAlgorithm, ExportCompressionAlgorithm? exportAlgorithm)
    {
        var backupPath = NewDataPath(suffix: "BackupFolder");
        IOExtensions.DeleteDirectory(backupPath);
        var exportFile = Path.Combine(backupPath, "export.ravendbdump");

        using (var store = GetDocumentStore(new Options
        {
            ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(x => x.ExportImport.CompressionAlgorithm)] = defaultAlgorithm.ToString()
        }))
        {
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Company { Name = "HR" }, "companies/1");
                await session.SaveChangesAsync();
            }

            var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions { CompressionAlgorithm = exportAlgorithm }, exportFile);
            await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(15));

            Assert.True(File.Exists(exportFile));

            await using (var fileStream = File.OpenRead(exportFile))
            await using (var backupStream = await BackupUtils.GetDecompressionStreamAsync(fileStream))
            {
                var buffer = new byte[1024];
                var read = await backupStream.ReadAsync(buffer, 0, buffer.Length); // validates if we picked appropriate decompression algorithm
                Assert.True(read > 0);

                switch (exportAlgorithm)
                {
                    case null:
                    case ExportCompressionAlgorithm.Gzip:
                        Assert.IsType<GZipStream>(backupStream);
                        break;
                    case ExportCompressionAlgorithm.Zstd:
                        Assert.IsType<ZstdStream>(backupStream);
                        break;
                    default:
                        throw new ArgumentOutOfRangeException(nameof(defaultAlgorithm), defaultAlgorithm, null);
                }
            }
        }

        using (var store = GetDocumentStore())
        {
            var operation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), exportFile);
            await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(15));

            using (var session = store.OpenAsyncSession())
            {
                var company = await session.LoadAsync<Company>("companies/1");

                Assert.NotNull(company);
                Assert.Equal("HR", company.Name);
            }
        }
    }

    [RavenTheory(RavenTestCategory.BackupExportImport)]
    [InlineData(BackupCompressionAlgorithm.Gzip, BackupType.Backup)]
    [InlineData(BackupCompressionAlgorithm.Zstd, BackupType.Backup)]
    //[InlineData(BackupCompressionAlgorithm.Gzip, BackupType.Snapshot)]
    //[InlineData(BackupCompressionAlgorithm.Zstd, BackupType.Snapshot)]
    public async Task CanBackupRestore(BackupCompressionAlgorithm algorithm, BackupType backupType)
    {
        var backupPath = NewDataPath(suffix: "BackupFolder");
        IOExtensions.DeleteDirectory(backupPath);

        using (var store = GetDocumentStore(new Options
        {
            ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(x => x.Backup.CompressionAlgorithm)] = algorithm.ToString()
        }))
        {
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Company { Name = "HR" }, "companies/1");
                await session.SaveChangesAsync();
            }

            var config = Backup.CreateBackupConfiguration(backupPath, backupType);
            await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);

            var databaseName = GetDatabaseName() + "restore";

            var backupDirectory = Directory.GetDirectories(backupPath).First();
            var files = Directory.GetFiles(backupDirectory)
                .Where(Raven.Client.Documents.Smuggler.BackupUtils.IsFullBackupOrSnapshot)
                .OrderBackups()
                .ToArray();

            var lastFile = files.Last();

            Assert.True(File.Exists(lastFile));

            await using (var fileStream = File.OpenRead(lastFile))
            await using (var backupStream = await BackupUtils.GetDecompressionStreamAsync(fileStream))
            {
                var buffer = new byte[1024];
                var read = await backupStream.ReadAsync(buffer, 0, buffer.Length); // validates if we picked appropriate decompression algorithm
                Assert.True(read > 0);

                switch (algorithm)
                {
                    case BackupCompressionAlgorithm.Gzip:
                        Assert.IsType<GZipStream>(backupStream);
                        break;
                    case BackupCompressionAlgorithm.Zstd:
                        Assert.IsType<ZstdStream>(backupStream);
                        break;
                    default:
                        throw new ArgumentOutOfRangeException(nameof(algorithm), algorithm, null);
                }
            }

            var restoreConfig = new RestoreBackupConfiguration()
            {
                BackupLocation = backupDirectory,
                DatabaseName = databaseName,
                LastFileNameToRestore = lastFile
            };

            var restoreOperation = new RestoreBackupOperation(restoreConfig);
            var operation = await store.Maintenance.Server.SendAsync(restoreOperation);
            await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

            using (var store2 = GetDocumentStore(new Options
            {
                CreateDatabase = false,
                ModifyDatabaseName = s => databaseName
            }))
            {
                using (var session = store2.OpenAsyncSession())
                {
                    var company = await session.LoadAsync<Company>("companies/1");

                    Assert.NotNull(company);
                    Assert.Equal("HR", company.Name);
                }
            }
        }
    }
}
