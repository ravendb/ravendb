using System;
using System.IO;
using Raven.Client.Documents.Operations.Backups;
using Sparrow.Backups;
using Tests.Infrastructure;
using Xunit;
using Voron;
using Voron.Global;
using Voron.Impl.Backup;
using Voron.Util.Settings;
using Xunit.Abstractions;

namespace FastTests.Voron.Backups
{
    public class Full : StorageTest
    {
        public Full(ITestOutputHelper output) : base(output)
        {
        }

        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.MaxLogFileSize = 1000 * Constants.Storage.PageSize;
            options.ManualFlushing = true;
        }

        [RavenTheory(RavenTestCategory.BackupExportImport)]
        [InlineData(SnapshotBackupCompressionAlgorithm.Deflate)]
        [InlineData(SnapshotBackupCompressionAlgorithm.Zstd)]
        public void CanBackupAndRestoreSmall(SnapshotBackupCompressionAlgorithm compressionAlgorithm)
        {
            RequireFileBasedPager();
            var random = new Random();
            var buffer = new byte[8192];
            random.NextBytes(buffer);

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                for (int i = 0; i < 2; i++)
                {
                    tree.Add("items/" + i, new MemoryStream(buffer));
                }

                tx.Commit();
            }

            Env.FlushLogToDataFile(); // force writing data to the data file

            // add more data to journal files
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                for (int i = 2; i < 4; i++)
                {
                    tree.Add("items/" + i, new MemoryStream(buffer));
                }

                tx.Commit();
            }

            Env.FlushLogToDataFile(); // force writing data to the data file - this won't sync data to disk because there was another sync within last minute

            var voronDataDir = new VoronPathSetting(DataDir);

            BackupMethods.Full.ToFile(Env, voronDataDir.Combine("voron-test.backup"), compressionAlgorithm);

            BackupMethods.Full.Restore(voronDataDir.Combine("voron-test.backup"), voronDataDir.Combine("backup-test.data"));

            var options = StorageEnvironmentOptions.ForPathForTests(Path.Combine(DataDir, "backup-test.data"));
            options.MaxLogFileSize = Env.Options.MaxLogFileSize;

            using (var env = new StorageEnvironment(options))
            {
                using (var tx = env.ReadTransaction())
                {
                    var tree = tx.CreateTree("foo");
                    for (int i = 0; i < 4; i++)
                    {
                        var readResult = tree.Read("items/" + i);
                        Assert.NotNull(readResult);
                        var memoryStream = new MemoryStream();
                        readResult.Reader.CopyTo(memoryStream);
                        Assert.Equal(memoryStream.ToArray(), buffer);
                    }
                }
            }
        }
    }
}
