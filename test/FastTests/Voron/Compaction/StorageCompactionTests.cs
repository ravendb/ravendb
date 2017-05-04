// -----------------------------------------------------------------------
//  <copyright file="StorageCompactionTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using FastTests.Voron.FixedSize;
using Sparrow;
using Sparrow.LowMemory;
using Xunit;
using Voron;
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


        [Theory]
        [InlineDataWithRandomSeed(250)]
        [InlineDataWithRandomSeed(500)]
        [InlineDataWithRandomSeed(1000)]
        public unsafe void ShouldPreserveTables(int entries, int seed)
        {
            // Create random docs to check everything is preserved
            using (var allocator = new ByteStringContext(LowMemoryFlag.None))
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
                        schema.Create(tx, "test", 16);
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
                            TableValueReader reader;
                            var hasValue = table.ReadByKey(entry.Key, out reader);

                            if (delete.Contains(entry.Key))
                            {
                                // This key should not be here
                                Assert.False(hasValue);
                            }
                            else
                            {
                                // This key should be there
                                Assert.True(hasValue);

                                // Data should be the same
                                int size;
                                byte* ptr = reader.Read(0, out size);
                                Slice current;
                                using (Slice.External(allocator, ptr, size, out current))
                                    Assert.True(SliceComparer.Equals(current, entry.Key));

                                ptr = reader.Read(1, out size);
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
            var size = files
                .Where(x=>Path.GetFileNameWithoutExtension(x.Name) != "pending-recycle")
                .Sum(x => x.Length);

            var directories = d.GetDirectories();
            size += directories.Sum(x => GetDirSize(x));

            return size;
        }
    }
}