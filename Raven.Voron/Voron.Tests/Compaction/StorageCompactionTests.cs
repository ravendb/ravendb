// -----------------------------------------------------------------------
//  <copyright file="StorageCompactionTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Xunit;
using Voron.Impl.Compaction;

namespace Voron.Tests.Compaction
{
    public class StorageCompactionTests : IDisposable
    {
        public const string CompactionTestsData = "Data";
        public const string CompactedData = "Data.Compacted";

        public StorageCompactionTests()
        {
            Clean();
        }

        [PrefixesFact]
        public void CompactionMustNotLooseAnyData()
        {
            var treeNames = new List<string>();
            var multiValueTreeNames = new List<string>();

            var random = new Random();

            var value1 = new byte[random.Next(1024*1024*2)];
            var value2 = new byte[random.Next(1024*1024*2)];

            random.NextBytes(value1);
            random.NextBytes(value2);

            const int treeCount = 5;
            const int recordCount = 6;
            const int multiValueTreeCount = 7;
            const int multiValueRecordsCount = 4;
            const int multiValuesCount = 3;

            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(CompactionTestsData)))
            {
                for (int i = 0; i < treeCount; i++)
                {
                    using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                    {
                        string name = "tree/" + i;
                        treeNames.Add(name);

                        var tree = env.CreateTree(tx, name);

                        for (int j = 0; j < recordCount; j++)
                        {
                            tree.Add(string.Format("{0}/items/{1}", name, j), j%2 == 0 ? value1 : value2);
                        }

                        tx.Commit();
                    }
                }

                for (int i = 0; i < multiValueTreeCount; i++)
                {
                    using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                    {
                        var name = "multiValueTree/" + i;
                        multiValueTreeNames.Add(name);

                        var tree = env.CreateTree(tx, name);

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

            StorageCompaction.Execute(StorageEnvironmentOptions.ForPath(CompactionTestsData), (StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)StorageEnvironmentOptions.ForPath(CompactedData));

            using (var compacted = new StorageEnvironment(StorageEnvironmentOptions.ForPath(CompactedData)))
            {
                using (var tx = compacted.NewTransaction(TransactionFlags.Read))
                {
                    foreach (var treeName in treeNames)
                    {
                        var tree = compacted.CreateTree(tx, treeName);

                        for (int i = 0; i < recordCount; i++)
                        {
                            var readResult = tree.Read(string.Format("{0}/items/{1}", treeName, i));

                            Assert.NotNull(readResult);

                            if (i%2 == 0)
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
                        var tree = compacted.CreateTree(tx, treeName);

                        for (int i = 0; i < multiValueRecordsCount; i++)
                        {
                            var multiRead = tree.MultiRead("record/" + i);

                            Assert.True(multiRead.Seek(Slice.BeforeAllKeys));

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

        [PrefixesFact]
        public void ShouldOccupyLessSpace()
        {
            var r = new Random();
            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(CompactionTestsData)))
            {
                using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    var tree = env.CreateTree(tx, "records");

                    for (int i = 0; i < 100; i++)
                    {
                        var bytes = new byte[r.Next(10, 2*1024*1024)];
                        r.NextBytes(bytes);

                        tree.Add("record/" + i, bytes);
                    }

                    tx.Commit();
                }

                using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    var tree = env.CreateTree(tx, "records");

                    for (int i = 0; i < 50; i++)
                    {
                        tree.Delete("record/" + r.Next(0, 100));
                    }

                    tx.Commit();
                }
            }

            var oldSize = GetDirSize(new DirectoryInfo(CompactionTestsData));

            StorageCompaction.Execute(StorageEnvironmentOptions.ForPath(CompactionTestsData), (StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions) StorageEnvironmentOptions.ForPath(CompactedData));

            var newSize = GetDirSize(new DirectoryInfo(CompactedData));

            Assert.True(newSize < oldSize, string.Format("Old size: {0:#,#;;0} MB, new size {1:#,#;;0} MB", oldSize / 1024 / 1024, newSize / 1024 / 1024));
        }

        [PrefixesFact]
        public void CannotCompactStorageIfIncrementalBackupEnabled()
        {
            var envOptions = StorageEnvironmentOptions.ForPath(CompactionTestsData);
            envOptions.IncrementalBackupEnabled = true;
            using (var env = new StorageEnvironment(envOptions))
            {
                using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    var tree = env.CreateTree(tx, "records");

                    tree.Add("record/1", new byte[9]);
                    tree.Add("record/2", new byte[9]);

                    tx.Commit();
                }
            }

            var srcOptions = StorageEnvironmentOptions.ForPath(CompactionTestsData);
            srcOptions.IncrementalBackupEnabled = true;

            var invalidOperationException = Assert.Throws<InvalidOperationException>(() => StorageCompaction.Execute(srcOptions, (StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)StorageEnvironmentOptions.ForPath(CompactedData)));

            Assert.Equal(StorageCompaction.CannotCompactBecauseOfIncrementalBackup, invalidOperationException.Message);
        }

        [PrefixesFact]
        public void ShouldDeleteCurrentJournalEvenThoughItHasAvailableSpace()
        {
            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(CompactionTestsData)))
            {
                using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    var tree = env.CreateTree(tx, "fruits");

                    tree.Add("apple", new byte[123]);
                    tree.Add("orange", new byte[99]);

                    tx.Commit();
                }
            }

            StorageCompaction.Execute(StorageEnvironmentOptions.ForPath(CompactionTestsData), (StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)StorageEnvironmentOptions.ForPath(CompactedData));

            var compactedDir = new DirectoryInfo(CompactedData);

            var journalsAfterCompaction = compactedDir.GetFiles("*.journal").Select(x => x.Name).ToList();

            Assert.Equal(0, journalsAfterCompaction.Count);

            // ensure it can write more data

            using (var compacted = new StorageEnvironment(StorageEnvironmentOptions.ForPath(CompactedData)))
            {
                using (var tx = compacted.NewTransaction(TransactionFlags.ReadWrite))
                {
                    var tree = compacted.CreateTree(tx, "fruits");

                    tree.Add("peach", new byte[144]);
                }
            }
        }

        [PrefixesFact]
        public void ShouldReportProgress()
        {
            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(CompactionTestsData)))
            {
                using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    var tree = env.CreateTree(tx, "fruits");

                    tree.Add("apple", new byte[123]);
                    tree.Add("orange", new byte[99]);

                    var tree2 = env.CreateTree(tx, "vegetables");

                    tree2.Add("carrot", new byte[123]);
                    tree2.Add("potato", new byte[99]);

                    var tree3 = env.CreateTree(tx, "multi");

                    tree3.MultiAdd("fruits", "apple");
                    tree3.MultiAdd("fruits", "orange");


                    tree3.MultiAdd("vegetables", "carrot");
                    tree3.MultiAdd("vegetables", "carrot");

                    tx.Commit();
                }
            }

            var progressReport = new List<string>();

            StorageCompaction.Execute(StorageEnvironmentOptions.ForPath(CompactionTestsData), (StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)StorageEnvironmentOptions.ForPath(CompactedData), x => progressReport.Add(string.Format("Copied {0} of {1} records in '{2}' tree. Copied {3} of {4} trees.", x.CopiedTreeRecords, x.TotalTreeRecordsCount, x.TreeName, x.CopiedTrees, x.TotalTreeCount)));

            Assert.NotEmpty(progressReport);
            Assert.Contains("Copied 0 of 2 records in 'fruits' tree. Copied 0 of 3 trees.", progressReport);
            Assert.Contains("Copied 2 of 2 records in 'fruits' tree. Copied 1 of 3 trees.", progressReport);
            Assert.Contains("Copied 0 of 2 records in 'multi' tree. Copied 1 of 3 trees.", progressReport);
            Assert.Contains("Copied 2 of 2 records in 'multi' tree. Copied 2 of 3 trees.", progressReport);
            Assert.Contains("Copied 0 of 2 records in 'vegetables' tree. Copied 2 of 3 trees.", progressReport);
            Assert.Contains("Copied 2 of 2 records in 'vegetables' tree. Copied 3 of 3 trees.", progressReport);
        }

        public static long GetDirSize(DirectoryInfo d)
        {
            var files = d.GetFiles();
            var size = files.Sum(x => x.Length);

            var directories = d.GetDirectories();
            size += directories.Sum(x => GetDirSize(x));

            return size;
        }

        public void Dispose()
        {
            Clean();
        }

        private static void Clean()
        {
            if (Directory.Exists(CompactionTestsData))
                StorageTest.DeleteDirectory(CompactionTestsData);

            if (Directory.Exists(CompactedData))
                StorageTest.DeleteDirectory(CompactedData);
        }
    }
}
