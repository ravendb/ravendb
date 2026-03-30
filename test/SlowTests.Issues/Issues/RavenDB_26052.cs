using System;
using System.Collections.Generic;
using System.IO;
using FastTests.Voron;
using Sparrow.Server;
using Sparrow.Threading;
using Tests.Infrastructure;
using Voron;
using Voron.Data.BTrees;
using Voron.Data.Tables;
using Voron.Impl;
using Voron.Impl.Compaction;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_26052 : StorageTest
{
    public static readonly Slice IndexName;


    public RavenDB_26052(ITestOutputHelper output) : base(output)
    {
    }

    static RavenDB_26052()
    {
        using (StorageEnvironment.GetStaticContext(out var ctx))
        {
            Slice.From(ctx, "GlobalDynamicKeyIndex", ByteStringType.Immutable, out IndexName);
        }
    }

    [StorageIndexEntryKeyGenerator]
    private static unsafe ByteStringContext.Scope TestGenerateDynamicKey(Transaction tx, ref TableValueReader tvr, out Slice slice)
    {
        var ptr = tvr.Read(1, out var size);
        var scope = tx.Allocator.Allocate(size, out var buffer);

        var span = new Span<byte>(buffer.Ptr, buffer.Length);
        new ReadOnlySpan<byte>(ptr, size).CopyTo(span);

        slice = new Slice(buffer);
        return scope;
    }

    [RavenTheory(RavenTestCategory.Voron)]
    [InlineData(10, 5)]
    [InlineData(50, 13)]
    [InlineDataWithRandomSeed(-1, -1)]
    public unsafe void CompactionShouldSkipGlobalDynamicKeyIndex(int entriesCount, int duplicatedValuesCount, int seed = -1)
    {
        using (var allocator = new ByteStringContext(SharedMultipleUseFlag.None))
        {
            if (seed != -1)
            {
                var random = new Random(seed);
                
                entriesCount = random.Next(1, 200);
                duplicatedValuesCount = random.Next(1, entriesCount + 1);
            }
            
            var create = new List<(Slice, long)>();

            for (var i = 0; i < entriesCount; i++)
            {
                Slice.From(allocator, "test" + i, out Slice key);
                create.Add((key, i));
            }

            // add duplicates
            
            for (var i = 0; i < duplicatedValuesCount; i++)
            {
                Slice.From(allocator, "test" + (entriesCount + i), out Slice key);
                create.Add((key, i));
            }

            var globalDynamicKeyIndexDef = new TableSchema.DynamicKeyIndexDef
            {
                GenerateKey = TestGenerateDynamicKey,
                IsGlobal = true,
                Name = IndexName,
                SupportDuplicateKeys = true
            };
            var schema = new TableSchema()
                .DefineKey(new TableSchema.IndexDef
                {
                    StartIndex = 0,
                    Count = 1,
                    IsGlobal = false
                })
                .DefineIndex(globalDynamicKeyIndexDef);

            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPathForTests(DataDir)))
            {
                using (var tx = env.WriteTransaction())
                {
                    schema.Create(tx, "test", 16);
                    var table = tx.OpenTable(schema, "test");

                    foreach (var entry in create)
                    {
                        table.Set(new TableValueBuilder
                        {
                            entry.Item1,
                            entry.Item2
                        });
                    }

                    tx.Commit();
                }
            }

            var compactedData = Path.Combine(DataDir, "Compacted");
            StorageCompaction.Execute(StorageEnvironmentOptions.ForPathForTests(DataDir),
                (StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)
                StorageEnvironmentOptions.ForPathForTests(compactedData));

            using (var compacted = new StorageEnvironment(StorageEnvironmentOptions.ForPathForTests(compactedData)))
            {
                using (var tx = compacted.ReadTransaction())
                {
                    var table = tx.OpenTable(schema, "test");
                    Assert.Equal(entriesCount + duplicatedValuesCount, table.NumberOfEntries);

                    // ensure the global index got recreated upon compaction
                    var indexTree = tx.ReadTree(IndexName.ToString(), isIndexTree: true);
                    Assert.NotNull(indexTree);
                    
                    Assert.Equal(entriesCount, indexTree.State.Header.NumberOfEntries);

                    TreeIterator treeIterator = indexTree.Iterate(false);
                    
                    treeIterator.Seek(Slices.BeforeAllKeys);
                    
                    long totalGlobalIndexValues = 0;

                    // count the actual number of dynamic index entries
                    do
                    {
                        var entry = treeIterator.Current;
                        Assert.NotNull(entry);

                        totalGlobalIndexValues += table.GetCountOfMatchesFor(globalDynamicKeyIndexDef, treeIterator.CurrentKey);
                    } while (treeIterator.MoveNext());
                    
                    Assert.Equal(entriesCount + duplicatedValuesCount, totalGlobalIndexValues);
                }
            }
        }
    }
}
