using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using FastTests.Voron;
using FastTests.Voron.FixedSize;
using FastTests.Voron.Util;
using Raven.Server.Utils;
using Sparrow.Server;
using Voron;
using Voron.Data.BTrees;
using Voron.Impl.Compaction;
using Xunit;

namespace SlowTests.Voron.Compaction
{
    public class StorageCompactionTestsSlow : StorageTest
    {
        public StorageCompactionTestsSlow()
        {
            IOExtensions.DeleteDirectory(DataDir);

            var compactedData = Path.Combine(DataDir, "Compacted");

            IOExtensions.DeleteDirectory(compactedData);
        }


        [Theory]
        [InlineDataWithRandomSeed()]
        public void ShouldOccupyLessSpace(int seed)
        {
            var r = new Random(seed);
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

            var oldSize = GetDirSize(new DirectoryInfo(DataDir));
            storageEnvironmentOptions = StorageEnvironmentOptions.ForPath(DataDir);
            storageEnvironmentOptions.ManualFlushing = true;
            var compactedData = Path.Combine(DataDir, "Compacted");
            StorageCompaction.Execute(storageEnvironmentOptions,
                (StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)StorageEnvironmentOptions.ForPath(compactedData));

            var newSize = GetDirSize(new DirectoryInfo(compactedData));

            Assert.True(newSize < oldSize, string.Format("Old size: {0:#,#;;0} MB, new size {1:#,#;;0} MB", oldSize / 1024 / 1024, newSize / 1024 / 1024));
        }

        [Theory]
        [InlineDataWithRandomSeed()]
        public void CompactionMustNotLooseAnyData(int seed)
        {
            var treeNames = new List<string>();
            var multiValueTreeNames = new List<string>();

            var random = new Random(seed);

            var value1 = new byte[random.Next(1024 * 1024 * 2)];
            var value2 = new byte[random.Next(1024 * 1024 * 2)];

            random.NextBytes(value1);
            random.NextBytes(value2);

            const int treeCount = 5;
            const int recordCount = 6;
            const int multiValueTreeCount = 7;
            const int multiValueRecordsCount = 4;
            const int multiValuesCount = 3;

            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(DataDir)))
            {
                for (int i = 0; i < treeCount; i++)
                {
                    using (var tx = env.WriteTransaction())
                    {
                        string name = "tree/" + i;
                        treeNames.Add(name);

                        var tree = tx.CreateTree(name);

                        for (int j = 0; j < recordCount; j++)
                        {
                            tree.Add(string.Format("{0}/items/{1}", name, j), j % 2 == 0 ? value1 : value2);
                        }

                        tx.Commit();
                    }
                }

                for (int i = 0; i < multiValueTreeCount; i++)
                {
                    using (var tx = env.WriteTransaction())
                    {
                        var name = "multiValueTree/" + i;
                        multiValueTreeNames.Add(name);

                        var tree = tx.CreateTree(name);

                        for (int j = 0; j < multiValueRecordsCount; j++)
                        {
                            for (int k = 0; k < multiValuesCount; k++)
                            {
                                tree.MultiAdd("record/" + j, "value/" + k);
                            }
                        }

                        tx.Commit();
                    }
                }
            }

            var compactedData = Path.Combine(DataDir, "Compacted");
            StorageCompaction.Execute(StorageEnvironmentOptions.ForPath(DataDir),
                (StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)StorageEnvironmentOptions.ForPath(compactedData));

            using (var compacted = new StorageEnvironment(StorageEnvironmentOptions.ForPath(compactedData)))
            {
                using (var tx = compacted.ReadTransaction())
                {
                    foreach (var treeName in treeNames)
                    {
                        var tree = tx.CreateTree(treeName);

                        for (int i = 0; i < recordCount; i++)
                        {
                            var readResult = tree.Read(string.Format("{0}/items/{1}", treeName, i));

                            Assert.NotNull(readResult);

                            if (i % 2 == 0)
                            {
                                var readBytes = new byte[value1.Length];
                                readResult.Reader.Read(readBytes, 0, readBytes.Length);

                                Assert.Equal(value1, readBytes);
                            }
                            else
                            {
                                var readBytes = new byte[value2.Length];
                                readResult.Reader.Read(readBytes, 0, readBytes.Length);

                                Assert.Equal(value2, readBytes);
                            }
                        }
                    }

                    foreach (var treeName in multiValueTreeNames)
                    {
                        var tree = tx.CreateTree(treeName);

                        for (int i = 0; i < multiValueRecordsCount; i++)
                        {
                            var multiRead = tree.MultiRead("record/" + i);

                            Assert.True(multiRead.Seek(Slices.BeforeAllKeys));

                            int count = 0;
                            do
                            {
                                Assert.Equal("value/" + count, multiRead.CurrentKey.ToString());
                                count++;
                            } while (multiRead.MoveNext());

                            Assert.Equal(multiValuesCount, count);
                        }
                    }
                }
            }
        }

        [Theory]
        [InlineDataWithRandomSeed(123, 99)]
        [InlineDataWithRandomSeed(1024 * 1024 * 5, 1)]
        public void Streams_RavenDB_6510(int fooSize, int barSize, int seed)
        {
            var r = new Random(seed);

            var fooBytes = new byte[fooSize];
            var barBytes = new byte[barSize];

            r.NextBytes(fooBytes);
            r.NextBytes(barBytes);

            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(DataDir)))
            {
                using (var tx = env.WriteTransaction())
                {
                    var tree = tx.CreateTree("streams");

                    tree.AddStream("foo", new MemoryStream(fooBytes), "t4g");
                    tree.AddStream("bar", new MemoryStream(barBytes));

                    tx.Commit();
                }
            }

            var compactedData = Path.Combine(DataDir, "Compacted");
            StorageCompaction.Execute(StorageEnvironmentOptions.ForPath(DataDir),
                (StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)StorageEnvironmentOptions.ForPath(compactedData));

            using (var compacted = new StorageEnvironment(StorageEnvironmentOptions.ForPath(compactedData)))
            {
                using (var tx = compacted.WriteTransaction())
                {
                    var tree = tx.ReadTree("streams");

                    var fooStream = tree.ReadStream("foo");
                    Assert.Equal(fooSize, fooStream.Length);
                    Assert.Equal(fooBytes, fooStream.ReadData());
                    Assert.Equal("t4g", tree.GetStreamTag("foo"));

                    var barStream = tree.ReadStream("bar");
                    Assert.Equal(barSize, barStream.Length);
                    Assert.Equal(barBytes, barStream.ReadData());
                    Assert.Null(tree.GetStreamTag("bar"));
                }
            }
        }

        [Theory]
        [InlineDataWithRandomSeed(398, 345)]
        [InlineDataWithRandomSeed(217, 701)]
        public void Compressed_tree_RavenDB_6510(int iterationCount, int size, int seed)
        {
            var r = new Random(seed);

            var bytes = new byte[size];
            r.NextBytes(bytes);

            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(DataDir)))
            {
                using (var tx = env.WriteTransaction())
                {
                    tx.CreateTree("tree", flags: TreeFlags.LeafsCompressed);

                    tx.Commit();
                }

                using (var tx = env.WriteTransaction())
                {
                    var tree = tx.ReadTree("tree");

                    for (var i = 0; i < iterationCount; i++)
                    {
                        tree.Add(i.ToString(), new MemoryStream(bytes));
                    }

                    tx.Commit();
                }
            }

            var compactedData = Path.Combine(DataDir, "Compacted");
            StorageCompaction.Execute(StorageEnvironmentOptions.ForPath(DataDir),
                (StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)StorageEnvironmentOptions.ForPath(compactedData));

            using (var compacted = new StorageEnvironment(StorageEnvironmentOptions.ForPath(compactedData)))
            {
                using (var tx = compacted.WriteTransaction())
                {
                    var tree = tx.ReadTree("tree");

                    Assert.True(tree.IsLeafCompressionSupported);

                    for (var i = 0; i < iterationCount; i++)
                    {
                        Slice key;

                        using (Slice.From(tx.Allocator, i.ToString(), ByteStringType.Immutable, out key))
                        {
                            using (var readResult = tree.ReadDecompressed(key))
                            {
                                if (readResult == null)
                                {

                                }

                                Assert.NotNull(readResult);

                                var result = readResult.Reader.ReadBytes(readResult.Reader.Length).ToArray();

                                Assert.Equal(bytes, result);
                            }
                        }
                    }
                }
            }
        }

        public static long GetDirSize(DirectoryInfo d)
        {
            var files = d.GetFiles();
            var size = files.Sum(x => x.Length);

            var directories = d.GetDirectories();
            size += directories.Sum(x => GetDirSize(x));

            return size;
        }
    }
}
