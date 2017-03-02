using System;
using System.IO;
using Voron;
using Voron.Impl.Compaction;
using Xunit;

namespace SlowTests.Voron.Compaction
{
    public class StorageCompactionTestsSlow : StorageTest
    {
        public StorageCompactionTestsSlow()
        {
            if (Directory.Exists(DataDir))
                StorageTest.DeleteDirectory(DataDir);

            var compactedData = Path.Combine(DataDir, "Compacted");
            if (Directory.Exists(compactedData))
                StorageTest.DeleteDirectory(compactedData);
        }


        [Fact]
        public void ShouldOccupyLessSpace()
        {
            var r = new Random();
            var storageEnvironmentOptions = StorageEnvironmentOptions.ForPath(DataDir);
            storageEnvironmentOptions.ManualFlushing = true;
            using (var env = new StorageEnvironment(storageEnvironmentOptions))
            {
                using (var tx = env.WriteTransaction())
                {
                    var tree = tx.CreateTree("records");

                    for (int i = 0; i < 100; i++)
                    {
                        var bytes = new byte[r.Next(10, 2 * 1024 * 1024)];
                        r.NextBytes(bytes);

                        tree.Add("record/" + i, bytes);
                    }

                    tx.Commit();
                }

                using (var tx = env.WriteTransaction())
                {
                    var tree = tx.CreateTree("records");

                    for (int i = 0; i < 50; i++)
                    {
                        tree.Delete("record/" + r.Next(0, 100));
                    }

                    tx.Commit();
                }
                env.FlushLogToDataFile();
            }

            var oldSize = StorageCompactionTests.GetDirSize(new DirectoryInfo(DataDir));
            storageEnvironmentOptions = StorageEnvironmentOptions.ForPath(DataDir);
            storageEnvironmentOptions.ManualFlushing = true;
            var compactedData = Path.Combine(DataDir, "Compacted");
            StorageCompaction.Execute(storageEnvironmentOptions,
                (StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)StorageEnvironmentOptions.ForPath(compactedData));

            var newSize = StorageCompactionTests.GetDirSize(new DirectoryInfo(compactedData));

            Assert.True(newSize < oldSize, string.Format("Old size: {0:#,#;;0} MB, new size {1:#,#;;0} MB", oldSize / 1024 / 1024, newSize / 1024 / 1024));
        }
    }
}