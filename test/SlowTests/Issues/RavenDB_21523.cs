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
using Raven.Server.Documents.PeriodicBackup.Restore;
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
    [InlineData(null, null)]
    [InlineData(null, null)]
    [InlineData(ExportCompressionAlgorithm.Gzip, null)]
    [InlineData(ExportCompressionAlgorithm.Zstd, null)]
    [InlineData(ExportCompressionAlgorithm.Gzip, CompressionLevel.Optimal)]
    [InlineData(ExportCompressionAlgorithm.Zstd, CompressionLevel.Optimal)]
    [InlineData(ExportCompressionAlgorithm.Gzip, CompressionLevel.Fastest)]
    [InlineData(ExportCompressionAlgorithm.Zstd, CompressionLevel.Fastest)]
    [InlineData(ExportCompressionAlgorithm.Gzip, CompressionLevel.SmallestSize)]
    [InlineData(ExportCompressionAlgorithm.Zstd, CompressionLevel.SmallestSize)]
    [InlineData(ExportCompressionAlgorithm.Gzip, CompressionLevel.NoCompression)]
    public async Task CanExportImport(ExportCompressionAlgorithm? algorithm, CompressionLevel? compressionLevel)
    {
        var backupPath = NewDataPath(suffix: "BackupFolder");
        IOExtensions.DeleteDirectory(backupPath);
        var exportFile = Path.Combine(backupPath, "export.ravendbdump");

        using (var store = GetDocumentStore(new Options
        {
            ModifyDatabaseRecord = record =>
            {
                record.Settings[RavenConfiguration.GetKey(x => x.ExportImport.CompressionAlgorithm)] = algorithm?.ToString();
                record.Settings[RavenConfiguration.GetKey(x => x.ExportImport.CompressionLevel)] = compressionLevel?.ToString();
            }
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
                    case null:
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
    [InlineData(BackupType.Backup, BackupCompressionAlgorithm.Gzip, CompressionLevel.Optimal)]
    [InlineData(BackupType.Snapshot, BackupCompressionAlgorithm.Gzip, CompressionLevel.Optimal)]
    [InlineData(BackupType.Backup, BackupCompressionAlgorithm.Zstd, CompressionLevel.Optimal)]
    [InlineData(BackupType.Snapshot, BackupCompressionAlgorithm.Zstd, CompressionLevel.Optimal)]
    [InlineData(BackupType.Backup, BackupCompressionAlgorithm.Gzip, CompressionLevel.Fastest)]
    [InlineData(BackupType.Snapshot, BackupCompressionAlgorithm.Gzip, CompressionLevel.Fastest)]
    [InlineData(BackupType.Backup, BackupCompressionAlgorithm.Zstd, CompressionLevel.Fastest)]
    [InlineData(BackupType.Snapshot, BackupCompressionAlgorithm.Zstd, CompressionLevel.Fastest)]
    [InlineData(BackupType.Backup, BackupCompressionAlgorithm.Gzip, CompressionLevel.SmallestSize)]
    [InlineData(BackupType.Snapshot, BackupCompressionAlgorithm.Gzip, CompressionLevel.SmallestSize)]
    [InlineData(BackupType.Backup, BackupCompressionAlgorithm.Zstd, CompressionLevel.SmallestSize)]
    [InlineData(BackupType.Snapshot, BackupCompressionAlgorithm.Zstd, CompressionLevel.SmallestSize)]
    [InlineData(BackupType.Backup, BackupCompressionAlgorithm.Gzip, CompressionLevel.NoCompression)]
    [InlineData(BackupType.Snapshot, BackupCompressionAlgorithm.Gzip, CompressionLevel.NoCompression)]
    public async Task CanBackupRestore(BackupType backupType, BackupCompressionAlgorithm algorithm, CompressionLevel compressionLevel)
    {
        var backupPath = NewDataPath(suffix: "BackupFolder");
        IOExtensions.DeleteDirectory(backupPath);

        using (var store = GetDocumentStore(new Options
        {
            ModifyDatabaseRecord = record =>
            {
                record.Settings[RavenConfiguration.GetKey(x => x.Backup.CompressionAlgorithm)] = algorithm.ToString();
                record.Settings[RavenConfiguration.GetKey(x => x.Backup.CompressionLevel)] = compressionLevel.ToString();
                record.Settings[RavenConfiguration.GetKey(x => x.Backup.SnapshotCompressionAlgorithm)] = GetSnapshotCompressionAlgorithm(algorithm).ToString();
                record.Settings[RavenConfiguration.GetKey(x => x.Backup.SnapshotCompressionLevel)] = compressionLevel.ToString();
            }
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

            await AssertBackupCompressionAlgorithm(backupType, algorithm, lastFile);

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

    private SnapshotBackupCompressionAlgorithm GetSnapshotCompressionAlgorithm(BackupCompressionAlgorithm algorithm)
    {
        switch (algorithm)
        {
            case BackupCompressionAlgorithm.Zstd:
                return SnapshotBackupCompressionAlgorithm.Zstd;
            case BackupCompressionAlgorithm.Gzip:
                return SnapshotBackupCompressionAlgorithm.Deflate;
            default:
                throw new ArgumentOutOfRangeException(nameof(algorithm), algorithm, null);
        }
    }

    private static async Task AssertBackupCompressionAlgorithm(BackupType backupType, BackupCompressionAlgorithm algorithm, string lastFile)
    {
        switch (backupType)
        {
            case BackupType.Backup:
                await using (var fileStream = File.OpenRead(lastFile))
                {
                    await using (var backupStream = await BackupUtils.GetDecompressionStreamAsync(fileStream))
                    {
                        var b = new byte[1024];
                        var read = await backupStream.ReadAsync(b, 0, b.Length); // validates if we picked appropriate decompression algorithm
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
                }
                break;

            case BackupType.Snapshot:
                // zip archive
                var buffer = new byte[4];

                using (var zip = ZipFile.Open(lastFile, ZipArchiveMode.Read, System.Text.Encoding.UTF8))
                {
                    foreach (var entry in zip.Entries)
                    {
                        if (entry.Name == RestoreSettings.SettingsFileName)
                            continue;

                        await using (var entryStream = entry.Open())
                        {
                            var read = await entryStream.ReadAsync(buffer, 0, buffer.Length);
                            if (read == 0)
                                throw new InvalidOperationException("Empty stream");

                            if (read == buffer.Length)
                            {
                                const uint zstdMagicNumber = 0xFD2FB528;
                                var readMagicNumber = BitConverter.ToUInt32(buffer);
                                if (readMagicNumber == zstdMagicNumber)
                                {
                                    Assert.Equal(BackupCompressionAlgorithm.Zstd, algorithm);
                                    return;
                                }
                            }

                            Assert.Equal(BackupCompressionAlgorithm.Gzip, algorithm);
                        }
                    }
                }

                break;

            default:
                throw new ArgumentOutOfRangeException(nameof(backupType), backupType, null);
        }
    }
}
