using System;
using System.Collections.Generic;
using FastTests.Voron;
using FastTests.Voron.FixedSize;
using Tests.Infrastructure;
using Voron.Data.CompactTrees;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron.CompactTrees;

public class CompactTreeSlowTests : StorageTest
{
    public CompactTreeSlowTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void CanStoreLargeNumberOfItemsInRandomlyOrder()
    {
        const int Size = 400000;

        using (var wtx = Env.WriteTransaction())
        {
            var tree = CompactTree.Create(wtx.LowLevelTransaction, "test");
            for (int i = 0; i < Size; i++)
            {
                tree.Add("hi" + i, i);
            }
            wtx.Commit();
        }
        using (var rtx = Env.ReadTransaction())
        {
            var tree = CompactTree.Create(rtx.LowLevelTransaction, "test");
            for (int i = 0; i < Size; i++)
            {
                Assert.True(tree.TryGetValue("hi" + i, out var r));
                Assert.Equal(i, r);
            }
        }
    }


    [RavenFact(RavenTestCategory.Voron)]
    public void CanDeleteLargeNumberOfItemsInRandomOrder()
    {
        const int Size = 400000;

        using (var wtx = Env.WriteTransaction())
        {
            var tree = CompactTree.Create(wtx.LowLevelTransaction, "test");
            for (int i = 0; i < Size; i++)
            {
                tree.Add("hi" + i, i);
            }
            wtx.Commit();
        }

        using (var wtx = Env.WriteTransaction())
        {
            var tree = CompactTree.Create(wtx.LowLevelTransaction, "test");
            for (int i = 0; i < Size; i++)
            {
                Assert.True(tree.TryRemove("hi" + i, out var v));
                Assert.Equal(i, v);
            }
            wtx.Commit();
        }
        using (var rtx = Env.ReadTransaction())
        {
            var tree = CompactTree.Create(rtx.LowLevelTransaction, "test");
            Assert.Equal(0, tree.State.NumberOfEntries);
            Assert.Equal(0, tree.State.BranchPages);
            Assert.Equal(1, tree.State.LeafPages);
        }
    }

    [RavenTheory(RavenTestCategory.Voron)]
    [InlineData(400000, 1337)]
    [InlineDataWithRandomSeed(400000)]
    public void CanDeleteLargeNumberOfItemsInRandomInsertionOrder(int size, int random)
    {
        HashSet<int> keys = new HashSet<int>();
        Random rnd = new Random(random);
        using (var wtx = Env.WriteTransaction())
        {
            var tree = CompactTree.Create(wtx.LowLevelTransaction, "test");
            for (int i = 0; i < size; i++)
            {
                int k = rnd.Next();
                if (!keys.Contains(k))
                {
                    tree.Add("hi" + k, i);
                    keys.Add(k);
                }
            }
            wtx.Commit();
        }

        keys = new HashSet<int>();
        rnd = new Random(random);
        using (var wtx = Env.WriteTransaction())
        {
            var tree = CompactTree.Create(wtx.LowLevelTransaction, "test");
            for (int i = 0; i < size; i++)
            {
                int k = rnd.Next();
                if (!keys.Contains(k))
                {
                    Assert.True(tree.TryRemove("hi" + k, out var v));
                    Assert.Equal(i, v);

                    keys.Add(k);
                }
            }
            wtx.Commit();
        }

        using (var rtx = Env.ReadTransaction())
        {
            var tree = CompactTree.Create(rtx.LowLevelTransaction, "test");
            Assert.Equal(0, tree.State.NumberOfEntries);
            Assert.Equal(0, tree.State.BranchPages);
            Assert.Equal(1, tree.State.LeafPages);
        }
    }


    [RavenFact(RavenTestCategory.Voron)]
    public void CanDeleteLargeNumberOfItemsFromStart()
    {
        const int Size = 400000;
        using (var wtx = Env.WriteTransaction())
        {
            var tree = CompactTree.Create(wtx.LowLevelTransaction, "test");
            for (int i = 0; i < Size; i++)
            {
                tree.Add($"hi{i:00000000}", i);
            }
            wtx.Commit();
        }

        using (var wtx = Env.WriteTransaction())
        {
            var tree = CompactTree.Create(wtx.LowLevelTransaction, "test");
            for (int i = 0; i < Size; i++)
            {
                Assert.True(tree.TryRemove($"hi{i:00000000}", out var v));
                Assert.Equal(i, v);
            }
            wtx.Commit();
        }
        using (var rtx = Env.ReadTransaction())
        {
            var tree = CompactTree.Create(rtx.LowLevelTransaction, "test");
            Assert.Equal(0, tree.State.NumberOfEntries);
            Assert.Equal(0, tree.State.BranchPages);
            Assert.Equal(1, tree.State.LeafPages);
        }
    }


    [RavenFact(RavenTestCategory.Voron)]
    public void CanDeleteLargeNumberOfItemsFromEnd()
    {
        const int Size = 400000;
        using (var wtx = Env.WriteTransaction())
        {
            var tree = CompactTree.Create(wtx.LowLevelTransaction, "test");
            for (int i = 0; i < Size; i++)
            {
                tree.Add($"hi{i:00000000}", i);
            }
            wtx.Commit();
        }

        using (var wtx = Env.WriteTransaction())
        {
            var tree = CompactTree.Create(wtx.LowLevelTransaction, "test");
            for (int i = Size - 1; i >= 0; i--)
            {
                Assert.True(tree.TryRemove($"hi{i:00000000}", out var v));
                Assert.Equal(i, v);
            }
            wtx.Commit();
        }
        using (var rtx = Env.ReadTransaction())
        {
            var tree = CompactTree.Create(rtx.LowLevelTransaction, "test");
            Assert.Equal(0, tree.State.NumberOfEntries);
            Assert.Equal(0, tree.State.BranchPages);
            Assert.Equal(1, tree.State.LeafPages);
        }
    }


    [RavenFact(RavenTestCategory.Voron)]
    public void CanStoreLargeNumberOfItemsInSequentialOrder()
    {
        const int Size = 400000;
        using (var wtx = Env.WriteTransaction())
        {
            var tree = CompactTree.Create(wtx.LowLevelTransaction, "test");
            for (int i = 0; i < Size; i++)
            {
                tree.Add($"hi{i:00000000}", i);
            }
            wtx.Commit();
        }
        using (var rtx = Env.ReadTransaction())
        {
            var tree = CompactTree.Create(rtx.LowLevelTransaction, "test");
            for (int i = 0; i < Size; i++)
            {
                var result = tree.TryGetValue($"hi{i:00000000}", out var r);
                Assert.True(result);
                Assert.Equal(i, r);
            }
        }
    }
}
