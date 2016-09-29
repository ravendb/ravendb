// -----------------------------------------------------------------------
//  <copyright file="StorageCompactionTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using FastTests.Voron.FixedSize;
using Lucene.Net.Search;
using Raven.Client.Linq;
using Sparrow;
using Xunit;
using Voron;
using Voron.Data;
using Voron.Data.Tables;
using Voron.Impl.Compaction;

namespace FastTests.Voron.Compaction
{
    public class StorageCompactionTests : StorageTest
    {
        public StorageCompactionTests()
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
            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(DataDir)))
            {
                using (var tx = env.WriteTransaction())
                {
                    var tree = tx.CreateTree(  "records");

                    for (int i = 0; i < 100; i++)
                    {
                        var bytes = new byte[r.Next(10, 2*1024*1024)];
                        r.NextBytes(bytes);

                        tree.Add("record/" + i, bytes);
                    }

                    tx.Commit();
                }

                using (var tx = env.WriteTransaction())
                {
                    var tree = tx.CreateTree( "records");

                    for (int i = 0; i < 50; i++)
                    {
                        tree.Delete("record/" + r.Next(0, 100));
                    }

                    tx.Commit();
                }
            }

            var oldSize = GetDirSize(new DirectoryInfo(DataDir));

            var compactedData = Path.Combine(DataDir, "Compacted");
            StorageCompaction.Execute(StorageEnvironmentOptions.ForPath(DataDir),
                (StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions) StorageEnvironmentOptions.ForPath(compactedData));

            var newSize = GetDirSize(new DirectoryInfo(compactedData));

            Assert.True(newSize < oldSize, string.Format("Old size: {0:#,#;;0} MB, new size {1:#,#;;0} MB", oldSize / 1024 / 1024, newSize / 1024 / 1024));
        }

        [Theory]
        [InlineDataWithRandomSeed(250)]
        [InlineDataWithRandomSeed(500)]
        [InlineDataWithRandomSeed(1000)]
        public unsafe void ShouldPreserveTables(int entries, int seed)
        {
            // Create random docs to check everything is preserved
            using (var allocator = new ByteStringContext())
            {
                var create = new Dictionary<Slice, long>();
                var delete = new List<Slice>();
                var r = new Random(seed);

                for (var i = 0; i < entries; i++)
                {
                    Slice key;
                    Slice.From(allocator, "test" + i, out key);

                    create.Add(key, r.Next());

                    if (r.NextDouble() < 0.5)
                    {
                        delete.Add(key);
                    }
                }

                // Create the schema
                var schema = new TableSchema()
                    .DefineKey(new TableSchema.SchemaIndexDef
                    {
                        StartIndex = 0,
                        Count = 1,
                        IsGlobal = false
                    });

                using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(DataDir)))
                {
                    // Create table in the environment
                    using (var tx = env.WriteTransaction())
                    {
                        schema.Create(tx, "test");
                        var table = tx.OpenTable(schema, "test");

                        foreach (var entry in create)
                        {
                            var value = entry.Value;

                            table.Set(new TableValueBuilder
                            {
                                entry.Key,
                                value
                            });
                        }

                        tx.Commit();
                    }

                    using (var tx = env.ReadTransaction())
                    {
                        var table = tx.OpenTable(schema, "test");
                        Assert.Equal(table.NumberOfEntries, entries);
                    }

                    // Delete some of the entries (this is so that compaction makes sense)
                    using (var tx = env.WriteTransaction())
                    {
                        var table = tx.OpenTable(schema, "test");

                        foreach (var entry in delete)
                        {
                            table.DeleteByKey(entry);
                        }

                        tx.Commit();
                    }
                }

                var compactedData = Path.Combine(DataDir, "Compacted");
                StorageCompaction.Execute(StorageEnvironmentOptions.ForPath(DataDir),
                    (StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)
                    StorageEnvironmentOptions.ForPath(compactedData));

                using (var compacted = new StorageEnvironment(StorageEnvironmentOptions.ForPath(compactedData)))
                {
                    using (var tx = compacted.ReadTransaction())
                    {
                        var table = tx.OpenTable(schema, "test");

                        foreach (var entry in create)
                        {
                            var value = table.ReadByKey(entry.Key);

                            if (delete.Contains(entry.Key))
                            {
                                // This key should not be here
                                Assert.Equal(null, value);
                            }
                            else
                            {
                                // This key should be there
                                Assert.NotEqual(null, value);

                                // Data should be the same
                                int size;
                                byte* ptr = value.Read(0, out size);
                                Slice current;
                                using (Slice.External(allocator, ptr, size, out current))
                                    Assert.True(SliceComparer.Equals(current, entry.Key));

                                ptr = value.Read(1, out size);
                                Assert.Equal(entry.Value, *(long*) ptr);
                            }
                        }

                        tx.Commit();
                    }
                }
            }
        }

        [Fact]
        public void CannotCompactStorageIfIncrementalBackupEnabled()
        {
            var envOptions = StorageEnvironmentOptions.ForPath(DataDir);
            envOptions.IncrementalBackupEnabled = true;
            using (var env = new StorageEnvironment(envOptions))
            {
                using (var tx = env.WriteTransaction())
                {
                    var tree = tx.CreateTree( "records");

                    tree.Add("record/1", new byte[9]);
                    tree.Add("record/2", new byte[9]);

                    tx.Commit();
                }
            }

            var srcOptions = StorageEnvironmentOptions.ForPath(DataDir);
            srcOptions.IncrementalBackupEnabled = true;

            var invalidOperationException = Assert.Throws<InvalidOperationException>(() => StorageCompaction.Execute(srcOptions,
                (StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)StorageEnvironmentOptions.ForPath(Path.Combine(DataDir, "Compacted"))));

            Assert.Equal(StorageCompaction.CannotCompactBecauseOfIncrementalBackup, invalidOperationException.Message);
        }

        [Fact]
        public void ShouldDeleteCurrentJournalEvenThoughItHasAvailableSpace()
        {
            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(DataDir)))
            {
                using (var tx = env.WriteTransaction())
                {
                    var tree = tx.CreateTree( "fruits");

                    tree.Add("apple", new byte[123]);
                    tree.Add("orange", new byte[99]);

                    tx.Commit();
                }
            }

            var compactedData = Path.Combine(DataDir, "Compacted");
            StorageCompaction.Execute(StorageEnvironmentOptions.ForPath(DataDir), 
                (StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)StorageEnvironmentOptions.ForPath(compactedData));

            var compactedDir = new DirectoryInfo(compactedData);

            var journalsAfterCompaction = compactedDir.GetFiles("*.journal").Select(x => x.Name).ToList();

            Assert.Equal(0, journalsAfterCompaction.Count);

            // ensure it can write more data

            using (var compacted = new StorageEnvironment(StorageEnvironmentOptions.ForPath(compactedData)))
            {
                using (var tx = compacted.WriteTransaction())
                {
                    var tree = tx.CreateTree( "fruits");

                    tree.Add("peach", new byte[144]);
                }
            }
        }

        [Fact]
        public void ShouldReportProgress()
        {
            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(DataDir)))
            {
                using (var tx = env.WriteTransaction())
                {
                    var tree = tx.CreateTree( "fruits");

                    tree.Add("apple", new byte[123]);
                    tree.Add("orange", new byte[99]);

                    var tree2 = tx.CreateTree( "vegetables");

                    tree2.Add("carrot", new byte[123]);
                    tree2.Add("potato", new byte[99]);

                    var tree3 = tx.CreateTree(  "multi");

                    tree3.MultiAdd("fruits", "apple");
                    tree3.MultiAdd("fruits", "orange");


                    tree3.MultiAdd("vegetables", "carrot");
                    tree3.MultiAdd("vegetables", "carrot");

                    tx.Commit();
                }
            }

            var progressReport = new List<string>();

            StorageCompaction.Execute(StorageEnvironmentOptions.ForPath(DataDir), 
                (StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)StorageEnvironmentOptions.ForPath(Path.Combine(DataDir, "Compacted")),
                x => progressReport.Add($"Copied {x.ObjectProgress} of {x.ObjectTotal} records in '{x.ObjectName}' tree. Copied {x.GlobalProgress} of {x.GlobalTotal} trees."));

            Assert.NotEmpty(progressReport);
            var lines = new[]
            {
                "Copied 0 of 2 records in '$Database-Metadata' tree. Copied 0 of 4 trees.",
                "Copied 2 of 2 records in '$Database-Metadata' tree. Copied 1 of 4 trees.",
                "Copied 0 of 2 records in 'fruits' tree. Copied 1 of 4 trees.",
                "Copied 2 of 2 records in 'fruits' tree. Copied 2 of 4 trees.",
                "Copied 0 of 2 records in 'multi' tree. Copied 2 of 4 trees.",
                "Copied 2 of 2 records in 'multi' tree. Copied 3 of 4 trees.",
                "Copied 0 of 2 records in 'vegetables' tree. Copied 3 of 4 trees.",
                "Copied 2 of 2 records in 'vegetables' tree. Copied 4 of 4 trees."
            };
            foreach (var line in lines)
            {
                Assert.Contains(line, lines);
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