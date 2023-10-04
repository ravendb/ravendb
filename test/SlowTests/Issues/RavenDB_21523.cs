using System;
using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Documents.Smuggler;
using Raven.Server.Config;
using Raven.Server.Config.Categories;
using Raven.Server.Utils;
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
    [InlineData(BackupCompressionAlgorithm.None)]
    [InlineData(BackupCompressionAlgorithm.Gzip)]
    [InlineData(BackupCompressionAlgorithm.Zstd)]
    public async Task CanExportImport(BackupCompressionAlgorithm algorithm)
    {
        var backupPath = NewDataPath(suffix: "BackupFolder");
        var exportFile = Path.Combine(backupPath, "export.ravendbdump");
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
                    case BackupCompressionAlgorithm.None:
                        Assert.IsType<BackupStream>(backupStream);
                        break;
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
}
