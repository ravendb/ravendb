using System;
using System.IO;
using System.Threading;
using FastTests.Voron;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace StressTests.Issues
{
    public class RavenDB_15930 : StorageTest
    {
        public RavenDB_15930(ITestOutputHelper output) : base(output)
        {
        }

        protected StorageEnvironmentOptions ModifyOptions(StorageEnvironmentOptions options, bool manualFlushing)
        {
            options.MaxLogFileSize = 8 * 1024 * 1024; // 8mb
            options.ManualFlushing = manualFlushing;

            return options;
        }

        [Fact]
        public void OnDatabaseRecoverShouldMarkLastJournalAsRecyclableIfItExceedMaxLogFileSize()
        {
            CreateAndPopulateTree(startWithBigTx: false);

            // restart
            using (var env = new StorageEnvironment(ModifyOptions(StorageEnvironmentOptions.ForPath(DataDir), manualFlushing: false)))
            {
                var journalPath = env.Options.JournalPath.FullPath;
                var journalsCount = new DirectoryInfo(journalPath).GetFiles().Length;
                Assert.True(SpinWait.SpinUntil(() => new DirectoryInfo(journalPath).GetFiles($"{StorageEnvironmentOptions.RecyclableJournalFileNamePrefix}*").Length == journalsCount,
                    TimeSpan.FromSeconds(30)));
            }
        }

        [Fact]
        public void ShouldNotReuseRecycledJournalIfItExceedMaxLogFileSizeOnSmallTxSize()
        {
            CreateAndPopulateTree(startWithBigTx: true);

            // restart
            using (var env = new StorageEnvironment(ModifyOptions(StorageEnvironmentOptions.ForPath(DataDir), manualFlushing: false)))
            {
                var journalPath = env.Options.JournalPath.FullPath;
                var journalsCount = new DirectoryInfo(journalPath).GetFiles().Length;
                Assert.True(SpinWait.SpinUntil(() => new DirectoryInfo(journalPath).GetFiles($"{StorageEnvironmentOptions.RecyclableJournalFileNamePrefix}*").Length == journalsCount,
                    TimeSpan.FromSeconds(30)));

                using (var tx = env.WriteTransaction())
                {
                    var bytes = new byte[1024];
                    new Random().NextBytes(bytes);

                    var tree = tx.ReadTree("items");
                    tree.Add("items/" + 2, bytes);

                    tx.Commit();
                }

                Assert.True(SpinWait.SpinUntil(() => new DirectoryInfo(journalPath).GetFiles($"{StorageEnvironmentOptions.RecyclableJournalFileNamePrefix}*").Length == 2,
                    TimeSpan.FromSeconds(30)));

                Assert.Equal(1, new DirectoryInfo(journalPath).GetFiles("000000000000000000*.journal").Length);
                Assert.Equal(3, new DirectoryInfo(journalPath).GetFiles().Length);
            }
        }

        [Fact]
        public void ShouldNotReuseRecycledJournalIfItExceedMaxLogFileSizeOnBigTxSize()
        {
            CreateAndPopulateTree(startWithBigTx: true);

            // restart
            using (var env = new StorageEnvironment(ModifyOptions(StorageEnvironmentOptions.ForPath(DataDir), manualFlushing: false)))
            {
                var journalPath = env.Options.JournalPath.FullPath;
                var journalsCount = new DirectoryInfo(journalPath).GetFiles().Length;
                Assert.True(SpinWait.SpinUntil(() => new DirectoryInfo(journalPath).GetFiles($"{StorageEnvironmentOptions.RecyclableJournalFileNamePrefix}*").Length == journalsCount,
                    TimeSpan.FromSeconds(30)));

                using (var tx = env.WriteTransaction())
                {
                    var bytes = new byte[8 * 1024 * 1024];
                    new Random().NextBytes(bytes);

                    var tree = tx.ReadTree("items");
                    tree.Add("items/" + 2, bytes);

                    tx.Commit();
                }

                Assert.True(SpinWait.SpinUntil(() => new DirectoryInfo(journalPath).GetFiles($"{StorageEnvironmentOptions.RecyclableJournalFileNamePrefix}*").Length == 2,
                    TimeSpan.FromSeconds(30)));

                Assert.Equal(1, new DirectoryInfo(journalPath).GetFiles("000000000000000000*.journal").Length);
                Assert.Equal(3, new DirectoryInfo(journalPath).GetFiles().Length);
            }
        }

        private void CreateAndPopulateTree(bool startWithBigTx)
        {
            using (var env = new StorageEnvironment(ModifyOptions(StorageEnvironmentOptions.ForPath(DataDir), manualFlushing: true)))
            {
                using (var tx = env.WriteTransaction())
                {
                    tx.CreateTree("items");
                    tx.Commit();
                }

                if (startWithBigTx)
                {
                    // tx with size bigger than MaxLogFileSize
                    using (var tx = env.WriteTransaction())
                    {
                        var bytes = new byte[9 * 1024 * 1024];
                        new Random().NextBytes(bytes);

                        var tree = tx.ReadTree("items");
                        tree.Add("items/" + 0, bytes);

                        tx.Commit();
                    }
                }

                using (var tx = env.WriteTransaction())
                {
                    var bytes = new byte[1024];
                    new Random().NextBytes(bytes);

                    var tree = tx.ReadTree("items");
                    tree.Add("items/" + 1, bytes);

                    tx.Commit();
                }

                // tx with size bigger than MaxLogFileSize
                using (var tx = env.WriteTransaction())
                {
                    var bytes = new byte[9 * 1024 * 1024];
                    new Random().NextBytes(bytes);

                    var tree = tx.ReadTree("items");
                    tree.Add("items/" + 322, bytes);

                    tx.Commit();
                }
            }
        }

    }
}
